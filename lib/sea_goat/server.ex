defmodule SeaGoat.Server do
  use GenServer
  alias SeaGoat.Server.MemTable
  alias SeaGoat.BloomFilter
  alias SeaGoat.WAL
  alias SeaGoat.Blocks
  alias SeaGoat.Tiers

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
    GenServer.call(server, {:transaction, f, self()})
  end

  @spec put(server :: GenServer.server(), key :: term(), value :: term()) :: :ok
  def put(server, key, value) do
    GenServer.call(server, {:put, key, value})
  end

  @spec remove(server :: GenServer.server(), key :: term()) :: :ok
  def remove(server, key) do
    GenServer.call(server, {:remove, key})
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
  def handle_call({:put, key, value}, _from, state) do
    command = {:put, key, value}
    WAL.append(state.wal, command)
    state = run_command(state, command)
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call({:remove, key}, _from, state) do
    command = {:remove, key}
    WAL.append(state.wal, command)
    state = run_command(state, command)
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call({:get, key}, from, state) do
    case read_in_memory(state, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, state}

      {:error, :not_found} ->
        start_read(state.tiers, key, from, state.lock_manager)
        {:noreply, state}
    end
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
        case try_wal(block, file, state.wal) do
          [] -> try_level(block, file)
          wal_result -> wal_result
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

    path = path(state.dir, state.block_count)

    case WAL.start_log(state.wal, path) do
      :ok ->
        {:noreply, state}

      e ->
        {:stop, e, state}
    end
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

      {:del, to_delete} ->
        Task.async(fn ->
          Enum.each(to_delete, &Blocks.delete_block(&1, state.lock_manager))
        end)

        state

      {:mv, from, to} ->
        Task.async(fn ->
          Blocks.switch(from, to, state.lock_manager)
        end)

        state

      {:merge, to_merge, path, tmp_path, tier} ->
        lock_manager = state.lock_manager

        %{ref: ref} =
          Task.async(fn ->
            Blocks.merge(Enum.map(to_merge, & &1.path), path, tmp_path, tier + 1, lock_manager)
          end)

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

        %{ref: ref} =
          Task.async(fn ->
            Blocks.flush(mem_table, path, tmp_path, @flush_tier, lock_manager)
          end)

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
        [prepend, append],
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
        commands = [{:del, [tmp_path]}, {:merge, to_merge, path, tmp_path, tier}]
        WAL.dump(acc.wal, path, commands)
        Enum.reduce(commands, %{acc | block_count: new_block_count}, &run_command(&2, &1))
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

  defp try_wal(block, file, wal) do
    case WAL.replay(wal, file) do
      {:ok, logs} -> [{:logs, logs, block, file}]
      _ -> []
    end
  end

  defp try_level(block, file) do
    case Blocks.fetch_level(file) do
      {:ok, bloom_filter, level} ->
        [{:level, bloom_filter, level, block, file}]

      _ ->
        []
    end
  end

  defp read_in_memory(state, key) do
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

  defp read_mem_tables([], _key), do: {:error, :not_found}

  defp read_mem_tables([mem_table | mem_tables], key) do
    case MemTable.read(mem_table, key) do
      {:value, value} -> {:ok, value}
      _ -> read_mem_tables(mem_tables, key)
    end
  end

  defp start_read(levels, key, from, lock_manager) do
    # move filter in to Blocks.read_file
    files_to_read =
      levels
      |> Enum.sort_by(fn {level, _tables} -> level end)
      |> Enum.flat_map(fn {_level, tables} ->
        tables
      end)
      |> Enum.filter(fn table ->
        BloomFilter.is_member(table.bloom_filter, key)
      end)
      |> Enum.map(&Map.get(&1, :path))

    Task.async(fn ->
      reply = Blocks.read_files(files_to_read, key, lock_manager)
      GenServer.reply(from, reply)
    end)
  end

  defp path(dir, block_count) do
    Path.join(dir, to_string(block_count) <> @file_suffix)
  end

  defp path_pair(path) do
    {path, path <> @tmp_file_suffix}
  end
end
