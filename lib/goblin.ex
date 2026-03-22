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

  @wal_suffix "wal"
  @disk_table_suffix "goblin"
  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10

  alias Goblin.{
    Manifest,
    WAL,
    DiskTable,
    MemTable,
    Compactor,
    Snapshots,
    Flusher,
    Tx,
    Export
  }

  defstruct [
    :name,
    :data_dir,
    :writer,
    :snapshots,
    :mem_table,
    :manifest,
    :wal,
    :file_counter,
    :flusher,
    :compactor,
    :mem_limit,
    write_queue: :queue.new(),
    wal_counter: 0
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
  @spec stop(GenServer.server()) :: :ok
  def stop(db) do
    GenServer.stop(db)
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
  @spec transaction(GenServer.server(), (Goblin.Tx.t() ->
                                           {:commit, Goblin.Tx.t(), any()} | :abort)) :: any()
  def transaction(db, callback) do
    ref = ext_ref(db)
    tx_key = make_ref()

    try do
      with :ok <- start_transaction(db, tx_key),
           {max_level_key, seq} <- Snapshots.register_tx(ref, tx_key),
           {:ok, tx, reply} <-
             run_transaction(db, Tx.Write.new(ref, tx_key, seq, max_level_key), callback),
           :ok <- commit_transaction(db, tx) do
        reply
      end
    after
      Snapshots.unregister_tx(ref, tx_key)
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
      {max_level_key, seq} = Snapshots.register_tx(ref, tx_key)

      Tx.Read.new(ref, tx_key, seq, max_level_key)
      |> callback.()
    after
      Snapshots.unregister_tx(ref, tx_key)
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
        {_max_level_key, seq} = Snapshots.register_tx(ref, tx_key)

        Snapshots.filter_tables(ref, tx_key)
        |> Enum.map(&Goblin.Queryable.stream(&1, min, max, seq))
      end,
      after: fn ->
        Snapshots.unregister_tx(ref, tx_key)
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
    snapshots = Snapshots.new()
    file_counter = :atomics.new(1, signed: false)

    File.exists?(data_dir) || File.mkdir_p!(data_dir)

    opts =
      args
      |> Keyword.put_new(:flush_level_file_limit, @default_flush_level_file_limit)
      |> Keyword.put_new(:level_base_size, @default_level_base_size)
      |> Keyword.put_new(:level_size_multiplier, @default_level_size_multiplier)
      |> Keyword.update(
        :max_sst_size,
        div(@default_level_base_size, @default_level_size_multiplier),
        & &1
      )
      |> Keyword.put(:next_file_f, fn -> next_file_pair(file_counter, data_dir) end)

    with {:ok, manifest} <- Manifest.open(name, data_dir) do
      %{dirt: dirt} = Manifest.snapshot(manifest, [:dirt])
      Enum.each(dirt, fn file -> File.exists?(file) && File.rm!(file) end)

      {:ok,
       %__MODULE__{
         name: name,
         data_dir: data_dir,
         snapshots: snapshots,
         manifest: manifest,
         flusher:
           Flusher.new(
             Keyword.take(opts, [
               :max_sst_size,
               :next_file_f,
               :bf_fpp,
               :bf_bit_array_size
             ])
           ),
         compactor:
           Compactor.new(
             Keyword.take(opts, [
               :max_sst_size,
               :next_file_f,
               :flush_level_file_limit,
               :level_base_size,
               :level_size_multiplier,
               :bf_fpp,
               :bf_bit_array_size
             ])
           ),
         mem_limit: Keyword.get(args, :mem_limit, @default_mem_limit),
         file_counter: file_counter
       }, {:continue, :restore_mem_table}}
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

    case schedule_write(state, tx_key, from, monitor_ref) do
      {:noop, state} -> {:noreply, state}
      {:write, state} -> {:reply, :ok, state}
    end
  end

  def handle_call({:commit_tx, tx}, {pid, _}, %{writer: {{pid, _}, _, monitor_ref}} = state) do
    Process.demonitor(monitor_ref)
    %{writes: writes, seq: seq} = tx

    with {:ok, wal} <- WAL.append(state.wal, writes),
         {:ok, manifest} <- Manifest.update(state.manifest, set_seq: seq) do
      Enum.each(writes, &apply_write(state.mem_table, &1))
      Snapshots.put_seq(state.snapshots, seq)
      {:reply, :ok, %{state | wal: wal, manifest: manifest}, {:continue, :next_writer}}
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
      active_disk_tables: disk_tables,
      active_wal: wal,
      retired_wals: retired_wals
    } = Manifest.snapshot(state.manifest, [:active_disk_tables, :active_wal, :retired_wals])

    filelist = List.flatten([state.manifest.log_file, wal, disk_tables, retired_wals])

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
  def handle_info({ref, {:ok, disk_tables, wal}}, %{flusher: %{ref: ref}} = state) do
    with {:ok, manifest} <-
           Manifest.update(state.manifest,
             activate_disk_tables: Enum.map(disk_tables, & &1.file),
             remove_wals: [wal.log_file]
           ),
         :ok <- WAL.rm(wal) do
      flusher = Flusher.set_ref(state.flusher, nil)

      compactor =
        Enum.reduce(
          disk_tables,
          state.compactor,
          fn disk_table, acc ->
            Snapshots.add_table(
              state.snapshots,
              disk_table.file,
              disk_table.level_key,
              disk_table,
              &File.rm(&1.file)
            )

            Compactor.put_into_level(acc, disk_table)
          end
        )

      Snapshots.soft_delete_table(state.snapshots, wal.log_file)
      state = %{state | manifest: manifest, flusher: flusher, compactor: compactor}
      {:noreply, state, {:continue, :next_flush}}
    else
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info({ref, {:error, reason}}, %{flusher: %{ref: ref}} = state) do
    {:stop, reason, state}
  end

  def handle_info(
        {ref, {:ok, disk_tables, retired_disk_tables}},
        %{compactor: %{ref: ref}} = state
      ) do
    with {:ok, manifest} <-
           Manifest.update(state.manifest,
             activate_disk_tables: Enum.map(disk_tables, & &1.file),
             remove_disk_tables: Enum.map(retired_disk_tables, & &1.file)
           ) do
      compactor =
        Enum.reduce(
          disk_tables,
          state.compactor,
          fn disk_table, acc ->
            Snapshots.add_table(
              state.snapshots,
              disk_table.file,
              disk_table.level_key,
              disk_table,
              &File.rm(&1.file)
            )

            Compactor.put_into_level(acc, disk_table)
          end
        )
        |> Compactor.set_ref(nil)
        |> then(fn compactor ->
          Enum.reduce(
            retired_disk_tables,
            compactor,
            fn disk_table, acc ->
              Snapshots.soft_delete_table(state.snapshots, disk_table.file)
              Compactor.remove_from_level(acc, disk_table)
            end
          )
        end)

      {:noreply, %{state | manifest: manifest, compactor: compactor},
       {:continue, :next_compaction}}
    else
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info({ref, {:error, reason}}, %{compactor: %{ref: ref}} = state) do
    {:stop, reason, state}
  end

  def handle_info({:retry_hard_delete, id}, state) do
    case Snapshots.hard_delete_table(state.snapshots, id) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_info({:DOWN, monitor_ref, _, pid, _}, %{writer: {{pid, _}, _, monitor_ref}} = state) do
    state = %{state | writer: nil}
    {:noreply, state, {:continue, :next_writer}}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{flusher: %{ref: ref}} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{compactor: %{ref: ref}} = state) do
    {:stop, reason, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_continue(:next_writer, state) do
    case :queue.out(state.write_queue) do
      {:empty, _write_queue} ->
        {:noreply, state, {:continue, :flush?}}

      {{:value, {from, _tx_key, _monitor_ref} = writer}, write_queue} ->
        GenServer.reply(from, :ok)
        state = %{state | writer: writer, write_queue: write_queue}
        {:noreply, state, {:continue, :flush?}}
    end
  end

  def handle_continue(:next_flush, state) do
    flusher =
      case Flusher.pop(state.flusher) do
        {:noop, flusher} ->
          flusher

        {:flush, mem_table, wal, flusher} ->
          start_flush(flusher, mem_table, wal)
      end

    {:noreply, %{state | flusher: flusher}, {:continue, :compact?}}
  end

  def handle_continue(:next_compaction, state) do
    compactor =
      case Compactor.pop(state.compactor) do
        {:noop, compactor} ->
          compactor

        {:compact, level_key, compactor} ->
          start_compaction(compactor, level_key)
      end

    {:noreply, %{state | compactor: compactor}, {:continue, :compact?}}
  end

  def handle_continue(:flush?, state) do
    if MemTable.size(state.mem_table) >= state.mem_limit do
      state = schedule_flush(state)

      case rotate_mem(state) do
        {:ok, state} -> {:noreply, state, {:continue, :clean_store}}
        {:error, reason} -> {:stop, reason, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_continue(:compact?, state) do
    state = schedule_compaction(state)
    {:noreply, state, {:continue, :clean_store}}
  end

  def handle_continue(:clean_store, state) do
    Snapshots.hard_delete_tables(state.snapshots)
    {:noreply, state}
  end

  def handle_continue(:restore_mem_table, state) do
    %{
      active_disk_tables: disk_tables,
      active_wal: wal,
      retired_wals: wals,
      seq: seq
    } = Manifest.snapshot(state.manifest, [:active_disk_tables, :active_wal, :retired_wals, :seq])

    max_file_count =
      disk_tables
      |> Enum.map(&parse_counter/1)
      |> Enum.max(fn -> 0 end)

    :atomics.put(state.file_counter, 1, max_file_count)

    Snapshots.put_seq(state.snapshots, seq)
    state = %{state | wal_counter: parse_counter(wal)}

    wals
    |> Enum.reduce_while({:ok, state}, fn wal, {:ok, acc} ->
      with {:ok, acc} <- replay_wal(acc, wal, seq, write?: false),
           :ok <- WAL.close(acc.wal) do
        {:cont, {:ok, schedule_flush(acc)}}
      else
        error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, state} ->
        case replay_wal(state, wal, seq, write?: true) do
          {:ok, state} ->
            {:noreply, %{state | wal_counter: state.wal_counter + 1},
             {:continue, :restore_disk_tables}}

          {:error, reason} ->
            {:stop, reason, state}
        end

      {:error, reason} ->
        {:stop, reason, state}
    end)
  end

  def handle_continue(:restore_disk_tables, state) do
    %{
      active_disk_tables: disk_tables
    } = Manifest.snapshot(state.manifest, [:active_disk_tables])

    Enum.reduce_while(disk_tables, {:ok, state}, fn disk_table, {:ok, acc} ->
      case DiskTable.from_file(disk_table) do
        {:ok, disk_table} ->
          compactor = Compactor.put_into_level(acc.compactor, disk_table)

          Snapshots.add_table(
            acc.snapshots,
            disk_table.file,
            disk_table.level_key,
            disk_table,
            &File.rm(&1.file)
          )

          {:cont, {:ok, %{acc | compactor: compactor}}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, state} ->
        Process.put(:"$goblin_ext_ref", state.snapshots)
        {:noreply, state, {:continue, :compact?}}

      {:error, reason} ->
        {:stop, reason, state}
    end)
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.wal, do: WAL.close(state.wal)
    Manifest.close(state.manifest)
    :ok
  end

  defp schedule_write(state, tx_key, from, monitor_ref) do
    cond do
      is_nil(state.writer) and :queue.is_empty(state.write_queue) ->
        {:write, %{state | writer: {from, tx_key, monitor_ref}}}

      true ->
        write_queue = :queue.in({from, tx_key, monitor_ref}, state.write_queue)
        {:noop, %{state | write_queue: write_queue}}
    end
  end

  defp schedule_flush(state) do
    case Flusher.push(state.flusher, state.mem_table, state.wal) do
      {:noop, flusher} ->
        %{state | flusher: flusher}

      {:flush, mem_table, wal, flusher} ->
        flusher = start_flush(flusher, mem_table, wal)
        %{state | flusher: flusher}
    end
  end

  defp schedule_compaction(state) do
    case Compactor.push(state.compactor) do
      {:noop, compactor} ->
        %{state | compactor: compactor}

      {:compact, level_key, compactor} ->
        compactor = start_compaction(compactor, level_key)
        %{state | compactor: compactor}
    end
  end

  defp rotate_mem(state) do
    wal_filename = new_file(state.data_dir, state.wal_counter, @wal_suffix)

    with :ok <- WAL.close(state.wal),
         {:ok, wal} <- WAL.open(state.name, wal_filename, true),
         {:ok, manifest} <-
           Manifest.update(state.manifest,
             activate_wal: wal.log_file,
             retire_wals: [state.wal.log_file]
           ) do
      mem_table = MemTable.new()
      Snapshots.add_table(state.snapshots, wal.log_file, -1, mem_table, &MemTable.delete/1)

      {:ok,
       %{
         state
         | mem_table: mem_table,
           wal: wal,
           manifest: manifest,
           wal_counter: state.wal_counter + 1
       }}
    end
  end

  defp replay_wal(state, nil, _max_seq, _opts) do
    wal = new_file(state.data_dir, state.wal_counter, @wal_suffix)
    mem_table = MemTable.new()
    Snapshots.add_table(state.snapshots, wal, -1, mem_table, &MemTable.delete/1)

    with {:ok, manifest} <- Manifest.update(state.manifest, activate_wal: wal),
         {:ok, wal} <- WAL.open(state.name, wal, true) do
      state = %{
        state
        | mem_table: mem_table,
          wal: wal,
          manifest: manifest
      }

      {:ok, state}
    end
  end

  defp replay_wal(state, wal, max_seq, opts) do
    mem_table = MemTable.new()
    Snapshots.add_table(state.snapshots, wal, -1, mem_table, &MemTable.delete/1)

    with {:ok, wal} <- WAL.open(state.name, wal, opts[:write?]) do
      WAL.replay(wal)
      |> Stream.filter(fn
        {:put, seq, _key, _value} when seq <= max_seq -> true
        {:remove, seq, _key} when seq <= max_seq -> true
        _ -> false
      end)
      |> Enum.each(&apply_write(mem_table, &1))

      {:ok, %{state | mem_table: mem_table, wal: wal}}
    end
  end

  defp start_flush(flusher, mem_table, wal) do
    %{ref: ref} = Task.async(fn -> Flusher.flush(flusher, mem_table, wal) end)
    Flusher.set_ref(flusher, ref)
  end

  defp start_compaction(compactor, level_key) do
    %{ref: ref} = Task.async(fn -> Compactor.compact(compactor, level_key) end)
    Compactor.set_ref(compactor, ref)
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

  defp apply_write(mem_table, {:put, seq, key, value}),
    do: MemTable.insert(mem_table, key, seq, value)

  defp apply_write(mem_table, {:remove, seq, key}),
    do: MemTable.remove(mem_table, key, seq)

  defp new_file(dir, count, suffix) do
    prefix =
      count
      |> Integer.to_string(16)
      |> String.pad_leading(20, "0")

    Path.join(dir, "#{prefix}.#{suffix}")
  end

  defp parse_counter(nil), do: 0

  defp parse_counter(path) do
    [n, _suffix] =
      path
      |> Path.basename()
      |> String.split(".")

    String.to_integer(n, 16)
  end

  defp next_file_pair(ref, dir) do
    count = :atomics.add_get(ref, 1, 1)
    file = new_file(dir, count, @disk_table_suffix)
    {"#{file}.tmp", file}
  end

  defp ext_ref(pid_or_name, timeout \\ 20000)
  defp ext_ref(_, timeout) when timeout <= 0, do: raise("no reference found")

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
