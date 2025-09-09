defmodule SeaGoat.Server do
  use GenServer
  alias SeaGoat.Server.MemTable
  alias SeaGoat.Server.Transaction
  alias SeaGoat.BloomFilter
  alias SeaGoat.WAL
  alias SeaGoat.Blocks
  alias SeaGoat.Tiers
  alias SeaGoat.LockManager

  @flush_tier 0
  @default_mem_limit 20000
  @default_level_limit 10
  @file_suffix ".seagoat"
  @tmp_file_suffix ".tmp"

  defstruct [
    :dir,
    :wal,
    :lock_manager,
    :limit,
    :level_limit,
    mem_table: MemTable.new(),
    transactions: %{},
    block_count: 0,
    tiers: Tiers.new()
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [:dir, :flusher, :merger, :lock_manager, :wal, :limit, :level_limit])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def stream(server) do
    GenServer.call(server, :stream)
  end

  def subscribe(server, pid \\ self()) do
    GenServer.call(server, {:subscribe, pid})
  end

  def transaction(server, f) do
    with {:ok, tx} <- start_transaction(server, self()) do
      run_transaction(server, f, tx)
    end
  end

  defp start_transaction(server, pid) do
    GenServer.call(server, {:start_transaction, pid})
  end

  defp run_transaction(server, f, tx) do
    case f.(tx) do
      {:commit, tx, reply} ->
        :ok = commit_transaction(server, tx, self())
        reply

      _ ->
        cancel_transaction(server, self())
    end
  end

  defp commit_transaction(server, tx, pid) do
    GenServer.call(server, {:commit_transaction, tx, pid})
  end

  defp cancel_transaction(server, pid) do
    GenServer.call(server, {:cancel_transaction, pid})
  end

  @spec put(server :: GenServer.server(), key :: term(), value :: term()) :: :ok
  def put(server, key, value) do
    transaction(server, fn tx ->
      tx = Transaction.put(tx, key, value)
      {:commit, tx, :ok}
    end)
  end

  @spec remove(server :: GenServer.server(), key :: term()) :: :ok
  def remove(server, key) do
    transaction(server, fn tx ->
      tx = Transaction.remove(tx, key)
      {:commit, tx, :ok}
    end)
  end

  @spec get(server :: GenServer.server(), key :: term()) :: {:ok, term()} | {:error, term()}
  def get(server, key) do
    GenServer.call(server, {:get, key})
  end

  @impl GenServer
  def init(opts) do
    dir = opts[:dir]

    with {:ok, files} <- File.ls(dir) do
      {:ok,
       %__MODULE__{
         dir: dir,
         wal: opts[:wal],
         lock_manager: opts[:lock_manager],
         mem_table: MemTable.new(),
         level_limit: opts[:level_limit] || @default_level_limit,
         limit: opts[:limit] || @default_mem_limit
       }, {:continue, {:replay, Enum.map(files, &Path.join(dir, &1))}}}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:get, key}, from, state) do
    case read_memory(state, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, state}

      :not_found ->
        read_disk(state.tiers, key, from, state.lock_manager)
        {:noreply, state}
    end
  end

  def handle_call({:start_transaction, pid}, _from, state) do
    if not Map.has_key?(state.transactions, pid) do
      tx = Transaction.make(pid)
      transactions = Map.put(state.transactions, pid, [])
      {:reply, {:ok, tx}, %{state | transactions: transactions}}
    else
      {:reply, {:error, :already_in_transaction}, state}
    end
  end

  def handle_call({:commit_transaction, tx, pid}, _from, state) do
    case Map.get(state.transactions, pid) do
      nil ->
        {:reply, {:error, :no_tx_found}, state}

      commits ->
        if not Transaction.is_in_conflict(tx, commits) do
          transactions =
            state.transactions
            |> Map.delete(pid)
            |> Enum.into(%{}, fn {pid, commits} ->
              {pid, [tx.mem_table | commits]}
            end)

          WAL.append_batch(state.wal, tx.writes)
          mem_table = MemTable.merge(state.mem_table, tx.mem_table)
          state = %{state | transactions: transactions, mem_table: mem_table}
          {:reply, :ok, state, {:continue, :flush}}
        else
          transactions = Map.delete(state.transactions, pid)
          state = %{state | transactions: transactions}
          {:reply, {:error, :in_conflict}, state}
        end
    end
  end

  def handle_call({:cancel_transaction, pid}, _from, state) do
    transactions = Map.delete(state.transactions, pid)
    state = %{state | transactions: transactions}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_continue(:flush, state) do
    {:noreply, maybe_flush(state)}
  end

  def handle_continue({:replay, files}, state) do
    state =
      files
      |> sort_files()
      |> Enum.flat_map(fn {block, file} ->
        case wal_or_db(file, state.wal) do
          {:ok, :logs, logs} ->
            [{:logs, logs, block, file}]

          {:ok, :level, bloom_filter, tier} ->
            [{:level, bloom_filter, tier, block, file}]

          _ ->
            []
        end
      end)
      |> Enum.reduce(state, fn
        {:logs, logs, block, _file}, acc ->
          logs
          |> Enum.reduce(%{acc | block_count: block}, &run_command(&2, &1))

        {:level, bloom_filter, tier, block, file}, acc ->
          tiers = Tiers.insert(acc.tiers, tier, file, nil, bloom_filter, nil)
          %{acc | tiers: tiers, block_count: block}
      end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({ref, {:flushed, bloom_filter, path, tmp_path}}, state) do
    tiers =
      Tiers.update(state.tiers, @flush_tier, fn
        %{state: {:flushing, ^ref}} = entry ->
          %{entry | mem_table: nil, bloom_filter: bloom_filter, state: nil}

        entry ->
          entry
      end)

    state =
      [{:mv, tmp_path, path}]
      |> Enum.reduce(%{state | tiers: tiers}, &run_command(&2, &1))
      |> maybe_merge()

    {:noreply, state}
  end

  def handle_info({ref, {:merged, merged, path, tmp_path, bloom_filter, tier}}, state) do
    tiers =
      Tiers.remove(state.tiers, tier - 1, fn
        %{state: {:merging, ^ref}} -> true
        _ -> false
      end)
      |> Tiers.insert(tier, path, nil, bloom_filter, nil)

    commands = [{:del, merged}, {:mv, tmp_path, path}]

    WAL.dump(state.wal, path, commands)

    state =
      commands
      |> Enum.reduce(%{state | tiers: tiers}, &run_command(&2, &1))
      |> maybe_merge()

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    # TODO: handle error of started task
    # on error, retry task (start with supervisor)...
    {:noreply, state}
  end

  defp run_command(state, command) do
    case command do
      {:put, key, value} ->
        mem_table = MemTable.upsert(state.mem_table, key, value)
        %{state | mem_table: mem_table}

      {:remove, key} ->
        mem_table = MemTable.delete(state.mem_table, key)
        %{state | mem_table: mem_table}

      {:del, files} ->
        wrap_in_lock(
          state.lock_manager,
          [],
          files,
          fn ->
            Enum.each(files, &Blocks.delete_block/1)
          end
        )

      {:mv, from, to} ->
        wrap_in_lock(
          state.lock_manager,
          [],
          [from, to],
          fn -> Blocks.switch(from, to) end
        )

      {:merge, to_merge, path, tmp_path, tier} ->
        lock_manager = state.lock_manager

        ref =
          lock_and_run(
            lock_manager,
            to_merge,
            [tmp_path],
            fn ->
              Blocks.merge(Enum.map(to_merge, & &1.path), path, tmp_path, tier + 1)
            end
          )

        tiers =
          Enum.reduce(to_merge, state.tiers, fn to_merge_entry, acc ->
            Tiers.update(acc, tier, fn entry ->
              if to_merge_entry.path == entry.path do
                %{entry | state: {:merging, ref}}
              else
                entry
              end
            end)
          end)

        %{state | tiers: tiers}

      {:flush, path, tmp_path} ->
        mem_table = state.mem_table
        lock_manager = state.lock_manager

        ref =
          lock_and_run(
            lock_manager,
            [],
            [tmp_path],
            fn ->
              Blocks.flush(mem_table, path, tmp_path, @flush_tier)
            end
          )

        tiers = Tiers.insert(state.tiers, @flush_tier, path, mem_table, nil, {:flushing, ref})

        %{
          state
          | mem_table: MemTable.new(),
            tiers: tiers
        }
    end
  end

  defp maybe_flush(state) do
    if MemTable.has_overflow(state.mem_table, state.limit) do
      new_block_count = state.block_count + 1
      {path, tmp_path} = WAL.current_file(state.wal) |> path_pair()
      new_path = path(state.dir, new_block_count)
      prepend = {:del, [tmp_path]}
      append = {:flush, path, tmp_path}
      WAL.rotate(state.wal, new_path, prepend, append)

      Enum.reduce(
        [append],
        %{state | block_count: new_block_count},
        &run_command(&2, &1)
      )
    else
      state
    end
  end

  defp maybe_merge(state) do
    state.tiers
    |> Tiers.tiers()
    |> Enum.reduce(state, fn tier, acc ->
      non_merging = Tiers.get_all(acc.tiers, tier, &(&1.state == nil))

      if length(non_merging) >= state.level_limit do
        to_merge =
          non_merging
          |> Enum.reverse()
          |> Enum.take(state.level_limit)

        new_block_count = acc.block_count + 1
        {path, tmp_path} = path(acc.dir, new_block_count) |> path_pair()
        del_command = {:del, [tmp_path]}
        merge_command = {:merge, to_merge, path, tmp_path, tier}
        WAL.dump(acc.wal, path, [del_command, merge_command])
        Enum.reduce([merge_command], %{acc | block_count: new_block_count}, &run_command(&2, &1))
      else
        acc
      end
    end)
  end

  defp sort_files(files) do
    files
    |> Enum.filter(&String.ends_with?(&1, @file_suffix))
    |> Enum.flat_map(fn file ->
      [block_count, _ext] =
        file
        |> Path.basename()
        |> String.split(".")

      case Integer.parse(block_count) do
        {int, _} -> [{int, file}]
        _ -> []
      end
    end)
    |> List.keysort(0)
  end

  defp wal_or_db(file, wal) do
    case WAL.replay(wal, file) do
      {:ok, logs} ->
        {:ok, :logs, logs}

      _ ->
        case Blocks.read_bloom_filter(file) do
          {:ok, {bloom_filter, tier}} ->
            {:ok, :level, bloom_filter, tier}

          _ ->
            {:error, :not_wal_or_db}
        end
    end
  end

  # defp try_wal(file, wal) do
  #   WAL.replay(wal, file)
  # end
  #
  # defp try_level(file) do
  #   case Blocks.read_bloom_filter(file) do
  #     {:ok, {bloom_filter, tier}} ->
  #       {:ok, bloom_filter, tier}
  #
  #     _ ->
  #       {:error, :not_a_db_file}
  #   end
  # end

  defp read_memory(state, key) do
    [
      state.mem_table
      | state.tiers
        |> Tiers.get_all(
          @flush_tier,
          fn entry -> match?({:flushing, _}, entry.state) end,
          fn entry -> entry.mem_table end
        )
    ]
    |> read_mem_tables(key)
  end

  defp read_mem_tables([], _key), do: :not_found

  defp read_mem_tables([mem_table | mem_tables], key) do
    case MemTable.read(mem_table, key) do
      {:value, value} -> {:ok, value}
      :not_found -> read_mem_tables(mem_tables, key)
    end
  end

  defp read_disk(tiers, key, from, lock_manager) do
    files =
      tiers
      |> Tiers.tiers()
      |> Enum.reduce([], fn tier, acc ->
        files =
          Tiers.get_all(
            tiers,
            tier,
            fn entry ->
              not match?({:flushing, _}, entry.state) &&
                BloomFilter.is_member(entry.bloom_filter, key)
            end,
            fn entry -> entry.path end
          )

        acc ++ files
      end)

    lock_and_run(
      lock_manager,
      files,
      [],
      fn ->
        case Blocks.read_key(files, key) do
          [{:ok, {:value, value}}] -> value
          _ -> nil
        end
      end,
      &GenServer.reply(from, &1)
    )
  end

  defp lock_and_run(lock_manager, public_resources, private_resources, run, post \\ & &1) do
    parent = self()
    lock_ref = make_ref()

    %{ref: ref} =
      Task.async(fn ->
        result =
          wrap_in_lock(
            lock_manager,
            public_resources,
            private_resources,
            fn ->
              send(parent, {:locked, lock_ref})
              run.()
            end
          )

        post.(result)
      end)

    receive do
      {:locked, ^lock_ref} -> :ok
    end

    ref
  end

  defp wrap_in_lock(lock_manager, public_resources, private_resources, run) do
    LockManager.lock(lock_manager, public_resources, :public)
    LockManager.lock(lock_manager, private_resources, :private)

    result = run.()

    for resource <- public_resources ++ private_resources,
        do: LockManager.unlock(lock_manager, resource)

    result
  end

  defp path(dir, block_count) do
    Path.join(dir, to_string(block_count) <> @file_suffix)
  end

  defp path_pair(path) do
    {path, path <> @tmp_file_suffix}
  end
end
