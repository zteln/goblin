defmodule Goblin do
  @moduledoc """
  A lightweight, embedded, LSM-tree database for Elixir.

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

        tx
        |> Goblin.Tx.put(:counter, counter + 1)
        |> Goblin.Tx.commit()
      end)
      # => :ok

  See `start_link/1` for configuration options.
  """
  @behaviour :gen_statem

  alias Goblin.{
    DiskTable,
    Export,
    FileIO,
    Merge,
    Levels,
    Manifest,
    MemTable,
    Tx,
    MVCC
  }

  @goblin_suffix "goblin"
  @wal_suffix "wal"

  @default_timeout :infinity

  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10
  @default_bit_array_size 10_000
  @default_fpp 0.01

  defstruct [
    :data_dir,
    :mem_table,
    :mvcc,
    :manifest,
    :file_counter,
    :opts,
    :sequence,
    :writer,
    levels: %{},
    flushing: %{},
    compacting: %{},
    readers: %{},
    writer_queue: :queue.new()
  ]

  @doc """
  Executes a read-write transaction.

  Transactions are executed serially and are ACID-compliant.
  The provided function receives a transaction struct and must return
  `{:commit, tx, reply}` to commit, or `{:abort, reply}` to abort.

  Calling `transaction` or any of its derivatives (`put`, `put_multi`, `remove`, `remove_multi`) on the same database from within a transaction raises.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `callback` - A function that takes a `Goblin.Tx.t()` and returns a transaction result
  - `opts` - A keyword list with options:
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `5000`)

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
      # => :error
  """
  def transaction(db, callback, opts \\ []) do
    mvcc = get_mvcc(db)
    tx_key = make_ref()

    case start_transaction(db, tx_key, opts) do
      :ok ->
        {seq, max_lk, tx_id} = MVCC.add_reader(mvcc, tx_key)

        tx = %Tx{
          mode: :write,
          mvcc: mvcc,
          tx_id: tx_id,
          sequence: seq,
          max_level_key: max_lk
        }

        result =
          try do
            callback.(tx)
          rescue
            exception ->
              cancel_transaction(db, tx_key, opts)
              reraise(exception, __STACKTRACE__)
          catch
            :throw, val ->
              cancel_transaction(db, tx_key, opts)
              throw(val)

            :exit, val ->
              cancel_transaction(db, tx_key, opts)
              exit(val)
          after
            MVCC.release_reader(mvcc, tx_key)
          end

        case result do
          {:commit, tx, reply} ->
            with :ok <- commit_transaction(db, tx, opts), do: reply

          {:abort, reply} ->
            cancel_transaction(db, tx_key, opts)
            reply

          _ ->
            cancel_transaction(db, tx_key, opts)
            raise "Invalid return from `Goblin.transaction/2`"
        end

      {:error, :nested_transaction} ->
        raise "cannot start a transaction from within a transaction"
    end
  end

  @doc """
  Writes a key-value pair to the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to be associated with `key`
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the key under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `5000`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put(db, :alice, "Alice")
      # => :ok

      Goblin.put(db, :alice, "Alice", tag: :admins)
      # => :ok
  """
  def put(db, key, val, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Tx.put(key, val, opts)
      |> Tx.commit()
    end)
  end

  @doc """
  Writes multiple key-value pairs in a single transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `pairs` - A list of `{key, value}` tuples
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `5000`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}])
      # => :ok
  """
  def put_multi(db, pairs, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Tx.put_multi(pairs, opts)
      |> Tx.commit()
    end)
  end

  @doc """
  Removes a key from the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `5000`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove(db, :alice)
      # => :ok

      Goblin.get(db, :alice)
      # => nil
  """
  def remove(db, key, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Tx.remove(key, opts)
      |> Tx.commit()
    end)
  end

  @doc """
  Removes multiple keys from the database in a single transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `5000`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove_multi(db, [:alice, :bob, :charlie])
      # => :ok
  """
  def remove_multi(db, keys, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Tx.remove_multi(keys, opts)
      |> Tx.commit()
    end)
  end

  @doc """
  Performs a read-only transaction.

  A snapshot is taken to provide a consistent mvcc of the database.
  Multiple readers run concurrently without blocking each other.
  Attempting to write within a read transaction raises.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `callback` - A function that takes a `Goblin.Tx.t()` struct

  ## Returns

  - The return value of `callback`

  ## Examples

      Goblin.read(db, fn tx ->
        alice = Goblin.Tx.get(tx, :alice)
        bob = Goblin.Tx.get(tx, :bob)
        {alice, bob}
      end)
      # => {"Alice", "Bob"}
  """
  def read(db, callback) do
    mvcc = get_mvcc(db)
    tx_key = make_ref()
    :gen_statem.cast(db, {:track_reader, self(), tx_key})
    {seq, max_lk, tx_id} = MVCC.add_reader(mvcc, tx_key)

    tx = %Tx{
      mode: :read,
      mvcc: mvcc,
      tx_id: tx_id,
      sequence: seq,
      max_level_key: max_lk
    }

    try do
      callback.(tx)
    after
      MVCC.release_reader(mvcc, tx_key)
      :gen_statem.cast(db, {:untrack_reader, tx_key})
    end
  end

  @doc """
  Retrieves the value associated with a key.

  Returns the default value if the key is not found.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

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
  def get(db, key, opts \\ []) do
    read(db, fn tx -> Tx.get(tx, key, opts) end)
  end

  @doc """
  Retrieves values for multiple keys in a single read.

  Keys not found in the database are excluded from the result.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A list of `{key, value}` tuples for keys found, in unspecified order

  ## Examples

      Goblin.get_multi(db, [:alice, :bob])
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

      Goblin.get_multi(db, [:alice, :nonexistent])
      # => [{:alice, "Alice"}]
  """
  def get_multi(db, keys, opts \\ []) do
    read(db, fn tx -> Tx.get_multi(tx, keys, opts) end)
  end

  @doc """
  Returns a stream of key-value pairs, optionally bounded by a range.
  Captures snapshots at enumeration.

  Entries are sorted by key in ascending order.
  Both `min` and `max` are inclusive.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `opts` - Keyword list of options:
    - `:min` - Minimum key, inclusive (optional)
    - `:max` - Maximum key, inclusive (optional)
    - `:tag` - Tag to filter by (optional)
    - `:limit` - Amount to take from the stream (optional)

  ## Returns

  - A stream of `{key, value}` tuples

  ## Examples

      Goblin.scan(db) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.scan(db, min: :bob) |> Enum.to_list()
      # => [{:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.scan(db, min: :alice, max: :bob) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

      Goblin.scan(db, limit: 2) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

      scan = Goblin.scan(db)
      Enum.to_list(scan)
      # => []
      Goblin.put(db, :alice, "Alice")
      Enum.to_list(scan)
      # => [{:alice, "Alice"}]
  """
  def scan(db, opts \\ []) do
    mvcc = get_mvcc(db)
    tx_key = make_ref()

    Tx.scan(
      fn ->
        :gen_statem.cast(db, {:track_reader, self(), tx_key})
        {seq, _max_lk, tx_id} = MVCC.add_reader(mvcc, tx_key)
        {seq, MVCC.get_tables(mvcc, tx_id)}
      end,
      Keyword.put(opts, :after, fn ->
        MVCC.release_reader(mvcc, tx_key)
        :gen_statem.cast(db, {:untrack_reader, tx_key})
      end)
    )
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
  @spec export(:gen_statem.server_ref(), Path.t(), keyword()) ::
          {:ok, Path.t()} | {:error, term()}
  def export(db, export_dir, opts \\ []) do
    :gen_statem.call(db, {:export, export_dir}, opts[:timeout] || @default_timeout)
  end

  @doc """
  Returns whether a memory-to-disk flush is currently running.
  """
  @spec flushing?(:gen_statem.server_ref(), keyword()) :: boolean()
  def flushing?(db, opts \\ []) do
    :gen_statem.call(db, :flushing?, opts[:timeout] || @default_timeout)
  end

  @doc """
  Returns whether background compaction is currently running.
  """
  @spec compacting?(:gen_statem.server_ref(), keyword()) :: boolean()
  def compacting?(db, opts \\ []) do
    :gen_statem.call(db, :compacting?, opts[:timeout] || @default_timeout)
  end

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
  def start_link(opts) do
    opts = Keyword.put_new(opts, :name, __MODULE__)

    with {:ok, db_opts, gen_statem_opts} <- split_opts(opts) do
      :gen_statem.start_link({:local, opts[:name]}, __MODULE__, db_opts, gen_statem_opts)
    end
  end

  @doc """
  Starts the database, see `start_link/1` for more details.
  """
  def start(opts) do
    opts = Keyword.put_new(opts, :name, __MODULE__)

    with {:ok, db_opts, gen_statem_opts} <- split_opts(opts) do
      :gen_statem.start({:local, opts[:name]}, __MODULE__, db_opts, gen_statem_opts)
    end
  end

  @doc """
  Stops the database.
  """
  @spec stop(:gen_statem.server_ref(), term(), timeout()) :: :ok
  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    :gen_statem.stop(db, reason, timeout)
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: opts[:name] || __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @impl :gen_statem
  def callback_mode, do: :state_functions

  @impl :gen_statem
  def terminate(_reason, _state, db) do
    for({_ref, {task, _}} <- db.flushing, do: Task.shutdown(task, :brutal_kill))
    for({_ref, {task, _}} <- db.compacting, do: Task.shutdown(task, :brutal_kill))
    :persistent_term.erase({__MODULE__, self()})
    db.manifest && Manifest.close(db.manifest)
    db.mem_table && MemTable.close(db.mem_table)
    :ok
  end

  @impl :gen_statem
  def init(args) do
    data_dir = args[:data_dir]
    file_counter = :atomics.new(1, signed: false)

    File.exists?(data_dir) || File.mkdir_p!(data_dir)

    opts =
      args
      |> Keyword.put_new(:bit_array_size, @default_bit_array_size)
      |> Keyword.put_new(:fpp, @default_fpp)
      |> Keyword.put_new(:mem_limit, @default_mem_limit)
      |> Keyword.put_new(:flush_level_file_limit, @default_flush_level_file_limit)
      |> Keyword.put_new(:level_base_size, @default_level_base_size)
      |> Keyword.put_new(:level_size_multiplier, @default_level_size_multiplier)
      |> Keyword.put_new(
        :max_sst_size,
        div(
          args[:level_base_size] || @default_level_base_size,
          args[:level_size_multiplier] || @default_level_size_multiplier
        )
      )

    db = %__MODULE__{
      data_dir: data_dir,
      file_counter: file_counter,
      mvcc: MVCC.new(),
      opts: opts
    }

    with {:ok, manifest} <- Manifest.open(data_dir),
         {:ok, db} <- handle_start(%{db | manifest: manifest}) do
      {:ok, :idle, db}
    end
  end

  @doc false
  def idle({:call, {pid, _} = from}, {:start_tx, tx_key}, db) do
    monitor_ref = Process.monitor(pid)
    writer = {tx_key, monitor_ref, from}
    {:next_state, :occupied, %{db | writer: writer}, [{:reply, from, :ok}]}
  end

  def idle({:call, from}, {:export, export_dir}, db) do
    reply = handle_export(db, export_dir)
    {:keep_state, db, [{:reply, from, reply}]}
  end

  def idle({:call, from}, :flushing?, db) do
    {:keep_state, db, [{:reply, from, db_flushing?(db)}]}
  end

  def idle({:call, from}, :compacting?, db) do
    {:keep_state, db, [{:reply, from, db_compacting?(db)}]}
  end

  def idle(:cast, {:track_reader, pid, tx_key}, db) do
    {:keep_state, handle_track_reader(db, pid, tx_key)}
  end

  def idle(:cast, {:untrack_reader, tx_key}, db) do
    {:next_state, :sweeping, handle_untrack_reader(db, tx_key),
     [{:next_event, :internal, :sweep}]}
  end

  def idle(:info, {ref, merge_result}, db) do
    case handle_merge(db, ref, merge_result) do
      {:ok, db} -> {:next_state, :sweeping, db, [{:next_event, :internal, :sweep}]}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  def idle(:info, {:DOWN, _, _, _, _} = down, db) do
    case handle_down(db, down) do
      {:ok, db} -> {:next_state, :sweeping, db, [{:next_event, :internal, :sweep}]}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  @doc false
  def occupied(:internal, :next_writer, db) do
    case :queue.out(db.writer_queue) do
      {:empty, _} ->
        {:next_state, :sweeping, db, [{:next_event, :internal, :sweep}]}

      {{:value, {_, _, from} = writer}, writer_queue} ->
        db = %{db | writer: writer, writer_queue: writer_queue}
        {:keep_state, db, [{:reply, from, :ok}]}
    end
  end

  def occupied(:internal, :flush, db) do
    case maybe_flush(db) do
      {:ok, db} -> {:keep_state, db, [{:next_event, :internal, :next_writer}]}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  def occupied({:call, {pid, _} = from}, {:start_tx, _}, %{writer: {_, _, {pid, _}}} = db) do
    {:keep_state, db, [{:reply, from, {:error, :nested_transaction}}]}
  end

  def occupied({:call, {pid, _} = from}, {:start_tx, tx_key}, db) do
    monitor_ref = Process.monitor(pid)
    writer = {tx_key, monitor_ref, from}
    writer_queue = :queue.in(writer, db.writer_queue)
    {:keep_state, %{db | writer_queue: writer_queue}}
  end

  def occupied({:call, from}, {:commit_tx, tx}, db) do
    new_seq = tx.sequence
    {_, monitor_ref, _} = db.writer
    Process.demonitor(monitor_ref, [:flush])
    commits = Enum.reverse(tx.commits)

    with :ok <- MemTable.append(db.mem_table, commits) do
      db = %{db | sequence: new_seq, writer: nil}
      publish_snapshot(db)
      {:keep_state, db, [{:reply, from, :ok}, {:next_event, :internal, :flush}]}
    else
      {:error, reason} = error -> {:stop_and_reply, reason, db, [{:reply, from, error}]}
    end
  end

  def occupied({:call, from}, {:cancel_tx, tx_key}, %{writer: {tx_key, _, _} = writer} = db) do
    {_, monitor_ref, _} = writer
    Process.demonitor(monitor_ref, [:flush])

    {:keep_state, %{db | writer: nil},
     [{:reply, from, :ok}, {:next_event, :internal, :next_writer}]}
  end

  def occupied({:call, from}, {:cancel_tx, _}, db) do
    {:keep_state, db, [{:reply, from, {:error, :not_writer}}]}
  end

  def occupied({:call, from}, {:export, export_dir}, db) do
    reply = handle_export(db, export_dir)
    {:keep_state, db, [{:reply, from, reply}]}
  end

  def occupied({:call, from}, :flushing?, db) do
    {:keep_state, db, [{:reply, from, db_flushing?(db)}]}
  end

  def occupied({:call, from}, :compacting?, db) do
    {:keep_state, db, [{:reply, from, db_compacting?(db)}]}
  end

  def occupied(:cast, {:track_reader, pid, tx_key}, db) do
    {:keep_state, handle_track_reader(db, pid, tx_key)}
  end

  def occupied(:cast, {:untrack_reader, tx_key}, db) do
    {:keep_state, handle_untrack_reader(db, tx_key)}
  end

  def occupied(:cast, {:abandon_tx, tx_key}, %{writer: {tx_key, ref, _}} = db) do
    Process.demonitor(ref, [:flush])
    {:keep_state, %{db | writer: nil}, [{:next_event, :internal, :next_writer}]}
  end

  def occupied(:cast, {:abandon_tx, tx_key}, db) do
    writer_queue =
      :queue.filter(
        fn {queued_tx_key, _, _} ->
          queued_tx_key != tx_key
        end,
        db.writer_queue
      )

    {:keep_state, %{db | writer_queue: writer_queue}}
  end

  def occupied(:info, {ref, merge_result}, db) do
    case handle_merge(db, ref, merge_result) do
      {:ok, db} -> {:keep_state, db}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  def occupied(:info, {:DOWN, _, _, _, _} = down, db) do
    case handle_down(db, down) do
      {:ok, %{writer: nil} = db} -> {:keep_state, db, [{:next_event, :internal, :next_writer}]}
      {:ok, db} -> {:keep_state, db}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  @doc false
  def sweeping(:internal, :sweep, db) do
    case MVCC.sweep(db.mvcc) do
      [] ->
        {:next_state, :idle, db}

      to_sweep ->
        with {:ok, paths} <- cleanup(to_sweep),
             {:ok, manifest} <- Manifest.sweep_dirt(db.manifest, paths) do
          {:next_state, :idle, %{db | manifest: manifest}}
        else
          {:error, reason} -> {:stop, reason, db}
        end
    end
  end

  defp handle_start(db) do
    manifest_file = Manifest.current_file(db.manifest)
    {manifest_seq, files, dirt} = Manifest.snapshot(db.manifest)
    orphans = find_orphans(db.data_dir, [manifest_file | Enum.map(files, &elem(&1, 1))])
    max_count = files |> Enum.map(&get_count_from_file/1) |> Enum.max(fn -> 0 end)
    :atomics.put(db.file_counter, 1, max_count + 1)
    db = %{db | sequence: manifest_seq}

    with {:ok, dirt} <- cleanup(dirt),
         {:ok, _} <- cleanup(orphans),
         {:ok, manifest} <- Manifest.sweep_dirt(db.manifest, dirt),
         {:ok, db} <- handle_restore(%{db | manifest: manifest}, files) do
      publish_snapshot(db)
      mark_ready(self(), db.mvcc)
      {:ok, db}
    end
  end

  defp handle_merge(db, ref, {:ok, new, old}) do
    if Map.has_key?(db.flushing, ref) or Map.has_key?(db.compacting, ref) do
      flushing = Map.delete(db.flushing, ref)
      compacting = Map.delete(db.compacting, ref)

      with {:ok, manifest} <- Manifest.update(db.manifest, tag(new), tag(old), db.sequence) do
        levels = Enum.reduce(new, db.levels, &Levels.put(&2, &1))

        db = %{
          db
          | levels: levels,
            manifest: manifest,
            flushing: flushing,
            compacting: compacting
        }

        publish_snapshot(db)
        {:ok, compact(db)}
      end
    else
      {:ok, db}
    end
  end

  defp handle_merge(db, ref, {:error, reason}) do
    if Map.has_key?(db.flushing, ref) or Map.has_key?(db.compacting, ref),
      do: {:error, reason},
      else: {:ok, db}
  end

  defp handle_export(db, export_dir) do
    {_, files, _} = Manifest.snapshot(db.manifest)
    files = Enum.map(files, &elem(&1, 1))
    manifest_file = Manifest.current_file(db.manifest)
    Export.into_tar(export_dir, [manifest_file | files])
  end

  defp handle_down(db, {_, ref, _, _, reason}) do
    cond do
      match?({_, ^ref, _}, db.writer) ->
        {tx_key, _, _} = db.writer
        MVCC.release_reader(db.mvcc, tx_key)
        {:ok, %{db | writer: nil}}

      Map.has_key?(db.flushing, ref) or Map.has_key?(db.compacting, ref) ->
        {:error, reason}

      Map.has_key?(db.readers, ref) ->
        {tx_key, readers} = Map.pop(db.readers, ref)
        MVCC.release_reader(db.mvcc, tx_key)
        {:ok, %{db | readers: readers}}

      true ->
        writer_queue =
          :queue.filter(
            fn
              {_, monitor_ref, _} -> monitor_ref != ref
            end,
            db.writer_queue
          )

        {:ok, %{db | writer_queue: writer_queue}}
    end
  end

  defp handle_track_reader(db, pid, tx_key) do
    monitor_ref = Process.monitor(pid)
    readers = Map.put(db.readers, monitor_ref, tx_key)
    %{db | readers: readers}
  end

  defp handle_untrack_reader(db, tx_key) do
    monitor_ref =
      Enum.find_value(db.readers, fn
        {monitor_ref, ^tx_key} -> monitor_ref
        _ -> false
      end)

    case monitor_ref do
      nil ->
        db

      monitor_ref ->
        Process.demonitor(monitor_ref, [:flush])
        readers = Map.delete(db.readers, monitor_ref)
        %{db | readers: readers}
    end
  end

  defp publish_snapshot(db) do
    flushing_mts = Map.values(db.flushing) |> Enum.map(fn {_task, mt} -> mt end)
    mem_tables = [db.mem_table | flushing_mts]

    levels =
      db.compacting
      |> Map.values()
      |> Enum.flat_map(fn {_task, dts} -> dts end)
      |> Enum.reduce(db.levels, &Levels.put(&2, &1))

    MVCC.put_snapshot(db.mvcc, mem_tables, levels, db.sequence)
  end

  defp handle_restore(db, []) do
    if is_nil(db.mem_table) do
      with {:ok, db} <- flush(db),
           {:ok, manifest} <- Manifest.update(db.manifest, tag([db.mem_table]), [], db.sequence) do
        {:ok, %{db | manifest: manifest}}
      end
    else
      with {:ok, db} <- maybe_flush(db) do
        {:ok, compact(db)}
      end
    end
  end

  defp handle_restore(db, [{:mem, file} | files]) do
    with {:ok, db} <- flush(db, file) do
      handle_restore(db, files)
    end
  end

  defp handle_restore(db, [{:disk, file} | files]) do
    with {:ok, dt} <- DiskTable.from_file(file) do
      levels = Levels.put(db.levels, dt)
      db = %{db | levels: levels}
      handle_restore(db, files)
    end
  end

  defp maybe_flush(db) do
    if MemTable.size(db.mem_table) >= db.opts[:mem_limit] do
      with {:ok, db} <- flush(db),
           {:ok, manifest} <- Manifest.update(db.manifest, tag([db.mem_table]), [], db.sequence) do
        db = %{db | manifest: manifest}
        publish_snapshot(db)
        {:ok, db}
      end
    else
      {:ok, db}
    end
  end

  defp flush(db, file \\ nil) do
    file = file || gen_file(db.file_counter, db.data_dir, @wal_suffix)

    with {:ok, seq, new_mt} <- MemTable.new(file),
         :ok <- close_wal(db.mem_table) do
      db =
        case db.mem_table do
          nil -> db
          mt -> merge(db, 0, [mt])
        end

      {:ok, %{db | mem_table: new_mt, sequence: max(db.sequence, seq)}}
    end
  end

  defp compact(%{compacting: compacting} = db) when map_size(compacting) == 0 do
    case Levels.next(db.levels, db.opts) do
      nil ->
        db

      {:merge, lk, dts, filter_tombstones?, levels} ->
        %{db | levels: levels} |> merge(lk, dts, filter_tombstones?)
    end
  end

  defp compact(db), do: db

  defp merge(db, lk, tables, filter_tombstones? \\ false) do
    opts = [
      level_key: lk,
      compress?: lk > 1,
      bit_array_size: db.opts[:bit_array_size],
      max_size: db.opts[:max_sst_size],
      fpp: db.opts[:fpp],
      filer: fn -> gen_file(db.file_counter, db.data_dir) end,
      filter_tombstones?: filter_tombstones?
    ]

    run_merge(db, tables, opts)
  end

  defp run_merge(db, [%MemTable{} = mt], opts) do
    task =
      Task.async(fn ->
        try do
          stream = MemTable.stream(mt)

          with {:ok, dts} <- DiskTable.build(stream, opts) do
            {:ok, dts, [mt]}
          end
        rescue
          e in Goblin.IOError -> {:error, e}
        end
      end)

    flushing = Map.put(db.flushing, task.ref, {task, mt})
    %{db | flushing: flushing}
  end

  defp run_merge(db, dts, opts) do
    task =
      Task.async(fn ->
        try do
          stream =
            Merge.stream(
              fn -> Enum.map(dts, &DiskTable.stream/1) end,
              filter_tombstones?: opts[:filter_tombstones?]
            )

          with {:ok, new_dts} <- DiskTable.build(stream, opts) do
            {:ok, new_dts, dts}
          end
        rescue
          e in Goblin.IOError -> {:error, e}
        end
      end)

    compacting = Map.put(db.compacting, task.ref, {task, dts})
    %{db | compacting: compacting}
  end

  defp start_transaction(db, tx_key, opts) do
    :gen_statem.call(db, {:start_tx, tx_key}, opts[:timeout] || @default_timeout)
  catch
    :exit, reason ->
      :gen_statem.cast(db, {:abandon_tx, tx_key})
      exit(reason)
  end

  defp commit_transaction(db, tx, opts),
    do: :gen_statem.call(db, {:commit_tx, tx}, opts[:timeout] || @default_timeout)

  defp cancel_transaction(db, tx_key, opts),
    do: :gen_statem.call(db, {:cancel_tx, tx_key}, opts[:timeout] || @default_timeout)

  defp db_flushing?(db), do: map_size(db.flushing) != 0
  defp db_compacting?(db), do: map_size(db.compacting) != 0

  defp close_wal(nil), do: :ok
  defp close_wal(mt), do: MemTable.close(mt)

  defp cleanup(tables, acc \\ [])
  defp cleanup([], acc), do: {:ok, acc}

  defp cleanup([%MemTable{} = mt | rest], acc) do
    with :ok <- FileIO.remove(mt.id) do
      MemTable.destroy(mt)
      cleanup(rest, [mt.id | acc])
    end
  end

  defp cleanup([%DiskTable{} = dt | rest], acc) do
    with :ok <- FileIO.remove(dt.id) do
      cleanup(rest, [dt.id | acc])
    end
  end

  defp cleanup([path | rest], acc) do
    with :ok <- FileIO.remove(path) do
      cleanup(rest, [path | acc])
    end
  end

  defp find_orphans(dir, files) do
    File.ls!(dir)
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.reject(&File.dir?/1)
    |> Enum.reject(&(&1 in files))
    |> Enum.filter(&String.ends_with?(&1, @goblin_suffix))
  end

  defp gen_file(ref, dir, suffix \\ @goblin_suffix) do
    prefix =
      (:atomics.add_get(ref, 1, 1) - 1)
      |> Integer.to_string(16)
      |> String.pad_leading(20, "0")

    path = Path.join(dir, "#{prefix}.#{suffix}")
    if File.exists?(path), do: File.rm!(path)
    path
  end

  defp get_count_from_file({type, path}) do
    suffix =
      case type do
        :mem -> @wal_suffix
        :disk -> @goblin_suffix
      end

    path
    |> Path.basename(".#{suffix}")
    |> String.to_integer(16)
  end

  defp tag([]), do: []
  defp tag([table | tables]), do: [tag(table) | tag(tables)]
  defp tag(%MemTable{} = mt), do: {:mem, mt.id}
  defp tag(%DiskTable{} = dt), do: {:disk, dt.id}

  defp split_opts(opts) do
    {gen_statem_opts, db_opts} =
      Keyword.split(opts, [:timeout, :spawn_opt, :hibernate_after, :debug])

    case Keyword.get(db_opts, :data_dir) do
      nil ->
        {:error, :data_dir_not_provided}

      data_dir ->
        try do
          {:ok, Keyword.put(db_opts, :data_dir, to_string(data_dir)), gen_statem_opts}
        rescue
          Protocol.UndefinedError ->
            {:error, :data_dir_not_a_string}
        end
    end
  end

  defp mark_ready(db, ref), do: :persistent_term.put({__MODULE__, db}, ref)

  defp get_mvcc(db) do
    pid = if is_pid(db), do: db, else: Process.whereis(db)

    (pid && :persistent_term.get({__MODULE__, pid}, nil)) ||
      raise ArgumentError, "Goblin database #{inspect(db)} is not running or still starting"
  end
end
