defmodule Goblin do
  @moduledoc """
  A lightweight, embedded database for Elixir.

  Goblin is a persistent key-value store with ACID transactions, crash
  recovery, and automatic background compaction. It runs inside your
  application's supervision tree.

  ## Starting a database

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        data_dir: "/path/to/db"
      )

  ## Basic operations

      Goblin.put(db, :alice, "Alice")
      Goblin.get(db, :alice)
      # => "Alice"

      Goblin.remove(db, :alice)
      Goblin.get(db, :alice)
      # => nil

  ## Batch operations

      Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}])

      Goblin.get_multi(db, [:alice, :bob])
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

  ## Transactions

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)
        tx = Goblin.Tx.put(tx, :counter, counter + 1)
        {:commit, tx, :ok}
      end)
      # => :ok

  See `start_link/1` for configuration options.
  """
  use GenServer

  alias Goblin.{
    Manifest,
    Broker,
    MemTable,
    DiskTable,
    Tx,
    Flusher,
    Compactor,
    Export
  }

  @wal_suffix "wal"
  @disk_table_suffix "goblin"
  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10

  defstruct [
    :name,
    :data_dir,
    :writer,
    :broker,
    :mem_table,
    :manifest,
    :wal,
    :disk_table_counter,
    :flusher,
    :flushing,
    :compactor,
    :compacting,
    :mem_limit,
    write_queue: :queue.new()
  ]

  @doc """
  Starts the database.

  Creates the `data_dir` if it does not exist.

  ## Options

  - `:name` - Registered name for the database (optional, defaults to `Goblin`)
  - `:data_dir` - Directory path for database files (required)
  - `:mem_limit` - Bytes to buffer in memory before flushing to disk (default: 64 MB)
  - `:bf_fpp` - Bloom filter false positive probability (default: 0.01)

  ## Returns

  - `{:ok, pid}` - On successful start
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        data_dir: "/var/lib/myapp/db"
      )
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @doc """
  Starts the database, see `start_link/1` for more details.
  """
  @spec start(keyword()) :: GenServer.on_start()
  def start(opts) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    GenServer.start(__MODULE__, opts, name: opts[:name])
  end

  @doc """
  Stops the database.
  """
  @spec stop(GenServer.server(), atom(), non_neg_integer() | :infinity) :: :ok
  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(db, reason, timeout)
  end

  @doc """
  Executes a function within a write transaction.

  Transactions are executed serially and are ACID-compliant.
  The provided function receives a transaction struct and must return
  `{:commit, tx, reply}` to commit, or `:abort` to abort.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `callback` - A function that takes a `Goblin.Tx.t()` and returns a transaction result

  ## Returns

  - `reply` - The reply from `{:commit, tx, reply}` when committed
  - `{:error, :aborted}` - When the transaction is aborted

  ## Examples

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)
        tx
        |> Goblin.Tx.put(:counter, counter + 1)
        |> Goblin.Tx.commit()
      end)
      # => :ok

      Goblin.transaction(db, fn tx ->
        tx
        |> Goblin.Tx.abort()
      end)
      # => {:error, :aborted}
  """
  @spec transaction(
          GenServer.server(),
          (Goblin.Tx.t() -> {:commit, Goblin.Tx.t(), any()} | :abort)
        ) :: any()
  def transaction(db, callback) do
    ref = ext_ref(db)
    tx_key = make_ref()

    try do
      with :ok <- start_transaction(db, tx_key),
           {max_level_key, seq} <- Broker.register_tx(ref, tx_key),
           {:ok, tx, reply} <-
             run_transaction(db, Tx.Write.new(ref, tx_key, seq, max_level_key), callback),
           :ok <- commit_transaction(db, tx) do
        reply
      end
    after
      Broker.unregister_tx(ref, tx_key)
      GenServer.cast(db, {:clear_writer, tx_key})
      GenServer.cast(db, :try_clean_up)
    end
  end

  @doc """
  Writes a key-value pair to the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to associate with `key`
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the key under

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put(db, :alice, "Alice")
      # => :ok

      Goblin.put(db, :alice, "Alice", tag: :admins)
      # => :ok
  """
  @spec put(GenServer.server(), term(), term(), keyword()) :: :ok
  def put(db, key, value, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Goblin.Tx.put(key, value, opts)
      |> Goblin.Tx.commit()
    end)
  end

  @doc """
  Writes multiple key-value pairs in a single transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `pairs` - A list of `{key, value}` tuples
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}])
      # => :ok
  """
  @spec put_multi(GenServer.server(), list({term(), term()}), keyword()) :: :ok
  def put_multi(db, pairs, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Goblin.Tx.put_multi(pairs, opts)
      |> Goblin.Tx.commit()
    end)
  end

  @doc """
  Removes a key from the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove(db, :alice)
      # => :ok

      Goblin.get(db, :alice)
      # => nil
  """
  @spec remove(GenServer.server(), term(), keyword()) :: :ok
  def remove(db, key, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Goblin.Tx.remove(key, opts)
      |> Goblin.Tx.commit()
    end)
  end

  @doc """
  Removes multiple keys from the database in a single transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove_multi(db, [:alice, :bob, :charlie])
      # => :ok
  """
  @spec remove_multi(GenServer.server(), list(term()), keyword()) :: :ok
  def remove_multi(db, keys, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Goblin.Tx.remove_multi(keys, opts)
      |> Goblin.Tx.commit()
    end)
  end

  @doc """
  Performs a read transaction.

  A snapshot is taken to provide a consistent view of the database.
  Multiple readers run concurrently without blocking each other.
  Attempting to write within a read transaction raises.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `f` - A function that takes a `Goblin.Tx.t()` struct

  ## Returns

  - The return value of `f`

  ## Examples

      Goblin.read(db, fn tx ->
        alice = Goblin.Tx.get(tx, :alice)
        bob = Goblin.Tx.get(tx, :bob)
        {alice, bob}
      end)
      # => {"Alice", "Bob"}
  """
  @spec read(GenServer.server(), (Goblin.Tx.t() -> any())) :: any()
  def read(db, callback) do
    ref = ext_ref(db)
    tx_key = make_ref()

    try do
      {max_level_key, seq} = Broker.register_tx(ref, tx_key)

      Tx.Read.new(ref, tx_key, seq, max_level_key)
      |> callback.()
    after
      Broker.unregister_tx(ref, tx_key)
      GenServer.cast(db, :try_clean_up)
    end
  end

  @doc """
  Retrieves the value associated with a key.

  Returns the default value if the key is not found.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under
    - `:default` - Value to return if `key` is not found (default: `nil`)

  ## Returns

  - The value associated with the key, or `default` if not found

  ## Examples

      Goblin.get(db, :alice)
      # => "Alice"

      Goblin.get(db, :nonexistent)
      # => nil

      Goblin.get(db, :nonexistent, default: :not_found)
      # => :not_found

      Goblin.get(db, :alice, tag: :admins)
      # => "Alice"
  """
  @spec get(GenServer.server(), term(), keyword()) :: any()
  def get(db, key, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.get(tx, key, opts)
    end)
  end

  @doc """
  Retrieves values for multiple keys in a single read.

  Keys not found in the database are excluded from the result.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A list of `{key, value}` tuples for keys found, sorted by key

  ## Examples

      Goblin.get_multi(db, [:alice, :bob])
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

      Goblin.get_multi(db, [:alice, :nonexistent])
      # => [{:alice, "Alice"}]
  """
  @spec get_multi(GenServer.server(), list(term()), keyword()) :: list({term(), term()})
  def get_multi(db, keys, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.get_multi(tx, keys, opts)
    end)
  end

  @doc """
  Returns a stream of key-value pairs, optionally bounded by a range.

  Entries are sorted by key in ascending order.
  Both `min` and `max` are inclusive.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `opts` - Keyword list of options:
    - `:min` - Minimum key, inclusive (optional)
    - `:max` - Maximum key, inclusive (optional)
    - `:tag` - Tag to filter by (optional)

  ## Returns

  - A stream of `{key, value}` tuples

  ## Examples

      Goblin.scan(db) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.scan(db, min: :bob) |> Enum.to_list()
      # => [{:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.scan(db, min: :alice, max: :bob) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}]
  """
  @spec scan(GenServer.server(), keyword()) :: Enumerable.t({term(), term()})
  def scan(db, opts \\ []) do
    ref = ext_ref(db)
    tx_key = make_ref()
    min = opts[:min]
    max = opts[:max]
    tag = opts[:tag]

    {min, max} =
      cond do
        is_nil(tag) -> {min, max}
        is_nil(min) and is_nil(max) -> {min, max}
        is_nil(max) -> {{:"$goblin_tag", tag, min}, max}
        is_nil(min) -> {min, {:"$goblin_tag", tag, max}}
        true -> {{:"$goblin_tag", tag, min}, {:"$goblin_tag", tag, max}}
      end

    Goblin.Iterator.k_merge_stream(
      fn ->
        {_max_level_key, seq} = Broker.register_tx(ref, tx_key)

        Broker.filter_tables(ref, tx_key)
        |> Enum.map(&Goblin.Queryable.stream(&1, min, max, seq))
      end,
      after: fn ->
        Broker.unregister_tx(ref, tx_key)
        GenServer.cast(db, :try_clean_up)
      end,
      min: min,
      max: max
    )
    |> Stream.flat_map(fn
      {{:"$goblin_tag", ^tag, key}, _seq, value} -> [{key, value}]
      {{:"$goblin_tag", _tag, _key}, _seq, _value} when is_nil(tag) -> []
      {key, _seq, value} when is_nil(tag) -> [{key, value}]
      _ -> []
    end)
  end

  @doc """
  Exports a snapshot of the database as a `.tar.gz` archive.

  The archive can be unpacked and used as the `data_dir` for a new
  database instance, acting as a backup.

  The export is run inside the server,
  thus blocking file deletion and writes until completed.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `export_dir` - Directory to place the exported `.tar.gz` file

  ## Returns

  - `{:ok, export_path}` - Path to the created archive
  - `{:error, reason}` - If an error occurred

  ## Examples

      Goblin.export(db, "/backups")
      # => {:ok, "/backups/goblin_20260220T120000Z.tar.gz"}
  """
  @spec export(GenServer.server(), Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def export(db, export_dir) do
    GenServer.call(db, {:export, export_dir})
  end

  @doc """
  Returns whether a memory-to-disk flush is currently running.
  """
  @spec flushing?(GenServer.server()) :: boolean()
  def flushing?(db) do
    GenServer.call(db, :flushing?)
  end

  @doc """
  Returns whether background compaction is currently running.
  """
  @spec compacting?(GenServer.server()) :: boolean()
  def compacting?(db) do
    GenServer.call(db, :compacting?)
  end

  @impl GenServer
  def init(args) do
    name = args[:name]
    data_dir = args[:data_dir] || raise ":data_dir not provided"
    disk_table_counter = :atomics.new(1, signed: false)

    File.exists?(data_dir) || File.mkdir_p!(data_dir)

    opts =
      args
      |> Keyword.put_new(:mem_limit, @default_mem_limit)
      |> Keyword.put_new(:flush_level_file_limit, @default_flush_level_file_limit)
      |> Keyword.put_new(:level_base_size, @default_level_base_size)
      |> Keyword.put_new(:level_size_multiplier, @default_level_size_multiplier)
      |> Keyword.put_new(
        :max_sst_size,
        div(@default_level_base_size, @default_level_size_multiplier)
      )
      |> Keyword.put(:next_file_f, fn ->
        next_file_pair(disk_table_counter, data_dir)
      end)

    with {:ok, manifest} <- Manifest.open(name, data_dir) do
      %{dirt: dirt} = Manifest.snapshot(manifest, [:dirt])
      Enum.each(dirt, fn file -> File.exists?(file) && File.rm!(file) end)
      manifest = Manifest.clear_dirt(manifest)

      {:ok,
       %__MODULE__{
         name: name,
         data_dir: data_dir,
         manifest: manifest,
         broker: Broker.new(),
         flusher: Flusher.new(opts),
         compactor: Compactor.new(opts),
         disk_table_counter: disk_table_counter,
         mem_limit: opts[:mem_limit]
       }, {:continue, :restore_state}}
    end
  end

  @impl GenServer
  def handle_call(
        {:start_tx, _},
        {pid, _},
        %{writer: {{pid, _}, _, _}} = state
      ) do
    {:reply, {:error, :already_in_tx}, state}
  end

  def handle_call({:start_tx, tx_key}, {pid, _} = from, state) do
    monitor_ref = Process.monitor(pid)
    writer = {from, tx_key, monitor_ref}

    cond do
      is_nil(state.writer) and :queue.is_empty(state.write_queue) ->
        {:reply, :ok, %{state | writer: writer}}

      true ->
        write_queue = :queue.in(writer, state.write_queue)
        {:noreply, %{state | write_queue: write_queue}}
    end
  end

  def handle_call({:commit_tx, tx}, {pid, _}, %{writer: {{pid, _}, _, monitor_ref}} = state) do
    Process.demonitor(monitor_ref)
    %{writes: writes, sequence: seq} = tx

    with :ok <- MemTable.append_commits(state.mem_table, writes),
         {:ok, manifest} <- Manifest.update_sequence(state.manifest, seq),
         :ok <- Broker.put_sequence(state.broker, seq) do
      {:reply, :ok, %{state | manifest: manifest}, {:continue, :next_writer}}
    else
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  def handle_call({:commit_tx, _tx}, _from, state) do
    {:reply, {:error, :not_writer}, state}
  end

  def handle_call(:abort_tx, {pid, _}, %{writer: {{pid, _}, _, monitor_ref}} = state) do
    Process.demonitor(monitor_ref)
    state = %{state | writer: nil}
    {:reply, :ok, state, {:continue, :next_writer}}
  end

  def handle_call(:abort_tx, _from, state) do
    {:reply, {:error, :not_writer}, state}
  end

  def handle_call({:export, export_dir}, _from, state) do
    %{
      disk_tables: dts,
      wal: wal,
      wals: wals
    } = Manifest.snapshot(state.manifest, [:disk_tables, :wal, :wals])

    filelist = List.flatten([state.manifest.log_file, wal, dts, wals])

    case Export.into_tar(export_dir, filelist) do
      {:ok, output} -> {:reply, {:ok, output}, state}
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  def handle_call(:flushing?, _from, state) do
    {:reply, state.flusher.ref != nil, state}
  end

  def handle_call(:compacting?, _from, state) do
    {:reply, state.compactor.ref != nil, state}
  end

  @impl GenServer
  def handle_cast({:clear_writer, tx_key}, %{writer: {_, tx_key, monitor_ref}} = state) do
    Process.demonitor(monitor_ref)
    state = %{state | writer: nil}
    {:noreply, state, {:continue, :next_writer}}
  end

  def handle_cast({:clear_writer, _}, state) do
    {:noreply, state}
  end

  def handle_cast(:try_clean_up, state) do
    {:noreply, state, {:continue, :clean_store}}
  end

  @impl GenServer
  def handle_info({ref, {:ok, dts, mt}}, %{flushing: ref} = state) do
    with {:ok, manifest} <-
           Manifest.add_flush(
             state.manifest,
             Enum.map(dts, & &1.file),
             MemTable.wal_path(mt)
           ),
         :ok <- MemTable.remove_wal(mt) do
      compactor =
        Enum.reduce(dts, state.compactor, fn dt, acc ->
          Broker.add_table(state.broker, dt.file, dt.level_key, dt, &DiskTable.remove/1)

          Compactor.put_into_level(acc, dt)
        end)

      Broker.soft_delete_table(state.broker, MemTable.wal_path(mt))
      state = %{state | manifest: manifest, compactor: compactor, flushing: nil}
      {:noreply, state, {:continue, :flush}}
    else
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info({ref, {:error, reason}}, %{flushing: ref} = state),
    do: {:stop, reason, state}

  def handle_info({ref, {:ok, new_dts, old_dts}}, %{compacting: ref} = state) do
    with {:ok, manifest} <-
           Manifest.add_compaction(
             state.manifest,
             Enum.map(new_dts, & &1.file),
             Enum.map(old_dts, & &1.file)
           ) do
      compactor =
        Enum.reduce(new_dts, state.compactor, fn dt, acc ->
          Broker.add_table(state.broker, dt.file, dt.level_key, dt, &DiskTable.remove/1)
          Compactor.put_into_level(acc, dt)
        end)

      Enum.each(old_dts, &Broker.soft_delete_table(state.broker, &1.file))

      state = %{state | manifest: manifest, compactor: compactor, compacting: nil}
      {:noreply, state, {:continue, :flush}}
    else
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info({ref, {:error, reason}}, %{compacting: ref} = state),
    do: {:stop, reason, state}

  def handle_info({:retry_hard_delete, id}, state) do
    case Broker.hard_delete_table(state.broker, id) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_info({:DOWN, monitor_ref, _, pid, _}, %{writer: {{pid, _}, _, monitor_ref}} = state) do
    state = %{state | writer: nil}
    {:noreply, state, {:continue, :next_writer}}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{flusher: %{ref: ref}} = state),
    do: {:stop, reason, state}

  def handle_info({:DOWN, ref, _, _, reason}, %{compactor: %{ref: ref}} = state),
    do: {:stop, reason, state}

  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_continue(:next_writer, state) do
    case :queue.out(state.write_queue) do
      {:empty, _write_queue} ->
        {:noreply, state, {:continue, :rotate_mem_table}}

      {{:value, {from, _tx_key, _monitor_ref} = writer}, write_queue} ->
        GenServer.reply(from, :ok)
        state = %{state | writer: writer, write_queue: write_queue}
        {:noreply, state, {:continue, :rotate_mem_table}}
    end
  end

  def handle_continue(:rotate_mem_table, state) do
    case MemTable.rotate?(state.mem_table, state.mem_limit) do
      true ->
        %{wal_count: wal_count} = Manifest.snapshot(state.manifest, [:wal_count])
        wal_path = new_file(state.data_dir, wal_count, @wal_suffix)

        with {:ok, state} <- rotate_mem_table(state, wal_path),
             {:ok, manifest} <- Manifest.add_wal(state.manifest, wal_path) do
          {:noreply, %{state | manifest: manifest}, {:continue, :flush}}
        else
          {:error, reason} -> {:stop, reason, state}
        end

      false ->
        {:noreply, state, {:continue, :flush}}
    end
  end

  def handle_continue(:flush, %{flushing: nil} = state) do
    case Flusher.dequeue(state.flusher) do
      {:noop, flusher} ->
        {:noreply, %{state | flusher: flusher}, {:continue, :compact}}

      {:flush, mem_table, flusher} ->
        state =
          %{state | flusher: flusher}
          |> start_flush(mem_table)

        {:noreply, state, {:continue, :compact}}
    end
  end

  def handle_continue(:flush, state),
    do: {:noreply, state, {:continue, :compact}}

  def handle_continue(:compact, %{compacting: nil} = state) do
    case Compactor.next(state.compactor) do
      {:noop, compactor} ->
        {:noreply, %{state | compactor: compactor}, {:continue, :clean_store}}

      {:compact, target_level_key, sources, targets, filter_tombstones?, compactor} ->
        state =
          %{state | compactor: compactor}
          |> start_compaction(target_level_key, sources, targets, filter_tombstones?)

        {:noreply, state, {:continue, :clean_store}}
    end
  end

  def handle_continue(:compact, state),
    do: {:noreply, state, {:continue, :clean_store}}

  def handle_continue(:clean_store, state) do
    Broker.hard_delete_tables(state.broker)
    {:noreply, state}
  end

  def handle_continue(:restore_state, state) do
    %{
      wals: wals,
      wal: wal,
      disk_tables: disk_tables,
      sequence: sequence,
      disk_table_count: disk_table_count,
      wal_count: wal_count
    } =
      Manifest.snapshot(state.manifest, [
        :wals,
        :wal,
        :disk_tables,
        :sequence,
        :wal_count,
        :disk_table_count
      ])

    wals = wals ++ [wal || new_file(state.data_dir, wal_count, @wal_suffix)]

    disk_table_counter = :atomics.put(state.disk_table_counter, 1, disk_table_count)
    state = %{state | disk_table_counter: disk_table_counter}

    with {:ok, state} <- restore_mem_tables(state, wals, sequence),
         {:ok, state} <- restore_disk_tables(state, disk_tables),
         {:ok, state} <- restore_sequence(state, sequence),
         {:ok, state} <- add_new_wal_to_manifest(state, wal) do
      Process.put(:"$goblin_ext_ref", state.broker)
      {:noreply, state, {:continue, :rotate_mem_table}}
    else
      {:error, reason} -> {:stop, reason, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    state.manifest && Manifest.close(state.manifest)
    state.mem_table && MemTable.close(state.mem_table)
    :ok
  end

  defp restore_mem_tables(state, wals, sequence) do
    current_wal = List.last(wals)

    Enum.reduce_while(wals, {:ok, state}, fn wal, {:ok, acc} ->
      write? = wal == current_wal

      case rotate_mem_table(acc, wal, write?: write?, sequence: sequence) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        error -> {:halt, error}
      end
    end)
  end

  defp restore_disk_tables(state, dts) do
    dts
    |> Enum.reduce_while({:ok, state}, fn dt, {:ok, acc} ->
      with {:ok, dt} <- DiskTable.from_file(dt) do
        Broker.add_table(acc.broker, dt.file, dt.level_key, dt, &DiskTable.remove/1)
        compactor = Compactor.put_into_level(acc.compactor, dt)
        {:cont, {:ok, %{acc | compactor: compactor}}}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp restore_sequence(state, sequence) do
    with :ok <- Broker.put_sequence(state.broker, sequence) do
      {:ok, state}
    end
  end

  defp add_new_wal_to_manifest(state, nil) do
    with {:ok, manifest} <- Manifest.add_wal(state.manifest, MemTable.wal_path(state.mem_table)) do
      {:ok, %{state | manifest: manifest}}
    end
  end

  defp add_new_wal_to_manifest(state, _), do: {:ok, state}

  defp rotate_mem_table(state, wal_path, opts \\ [])

  defp rotate_mem_table(%{mem_table: nil} = state, wal_path, opts) do
    open_mem_table(state, wal_path, opts)
  end

  defp rotate_mem_table(state, wal_path, opts) do
    with :ok <- MemTable.close(state.mem_table) do
      state
      |> retire_mem_table()
      |> open_mem_table(wal_path, opts)
    end
  end

  defp open_mem_table(state, wal_path, opts) do
    with {:ok, mt} <- MemTable.open(state.name, wal_path, opts) do
      Broker.add_table(state.broker, MemTable.wal_path(mt), -1, mt, &MemTable.delete_table/1)

      {:ok, %{state | mem_table: mt}}
    end
  end

  defp retire_mem_table(state) do
    flusher = Flusher.enqueue(state.flusher, state.mem_table)
    %{state | flusher: flusher}
  end

  defp start_flush(state, mem_table) do
    %{flusher: flusher} = state
    %{ref: ref} = Task.async(fn -> Flusher.flush(flusher, mem_table) end)
    %{state | flushing: ref}
  end

  defp start_compaction(state, target_level_key, source_dts, target_dts, filter_tombstones?) do
    %{compactor: compactor} = state

    %{ref: ref} =
      Task.async(fn ->
        Compactor.compact(
          compactor,
          target_level_key,
          source_dts,
          target_dts,
          filter_tombstones?
        )
      end)

    %{state | compacting: ref}
  end

  defp start_transaction(db, tx_key) do
    GenServer.call(db, {:start_tx, tx_key})
  end

  defp run_transaction(db, tx, callback) do
    case callback.(tx) do
      {:commit, tx, reply} ->
        {:ok, Tx.Write.complete(tx), reply}

      :abort ->
        abort_transaction(db)
        {:error, :aborted}
    end
  end

  defp commit_transaction(db, tx) do
    GenServer.call(db, {:commit_tx, tx})
  end

  defp abort_transaction(db) do
    GenServer.call(db, :abort_tx)
  end

  defp next_file_pair(ref, dir) do
    count = :atomics.add_get(ref, 1, 1)
    file = new_file(dir, count, @disk_table_suffix)
    {"#{file}.tmp", file}
  end

  defp new_file(dir, count, suffix) do
    prefix =
      count
      |> Integer.to_string(16)
      |> String.pad_leading(20, "0")

    Path.join(dir, "#{prefix}.#{suffix}")
  end

  defp ext_ref(pid_or_name, timeout \\ 20000)
  defp ext_ref(_, timeout) when timeout <= 0, do: raise("no reference found")
  defp ext_ref(nil, _timeout), do: raise("no reference found")

  defp ext_ref(pid, timeout) when is_pid(pid) do
    {:dictionary, dictionary} = Process.info(pid, :dictionary)

    case Keyword.get(dictionary, :"$goblin_ext_ref") do
      nil ->
        Process.sleep(50)
        ext_ref(pid, timeout - 50)

      ext_ref ->
        ext_ref
    end
  end

  defp ext_ref(name, timeout), do: ext_ref(Process.whereis(name), timeout - 50)
end
