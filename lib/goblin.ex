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
  use GenServer

  alias Goblin.{
    DiskTable,
    Export,
    FileIO,
    KMerger,
    Levels,
    Manifest,
    MemTable,
    Tx,
    View
  }

  @goblin_suffix "goblin"
  @wal_suffix "wal"

  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10
  @default_bit_array_size 10_000
  @default_fpp 0.01

  defstruct [
    :data_dir,
    :mem_table,
    :view,
    :manifest,
    :file_counter,
    :compact_ref,
    :opts,
    levels: %{},
    flush_refs: MapSet.new()
  ]

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
  def transaction(db, callback) do
    GenServer.call(db, {:transaction, callback})
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
  def read(db, callback) do
    view = ext_ref(db)
    tx_key = make_ref()

    try do
      {max_lk, seq, tx_id} = View.acquire(view, tx_key)

      %Tx{
        mode: :read,
        view: view,
        tx_id: tx_id,
        sequence: seq,
        max_level_key: max_lk
      }
      |> callback.()
    after
      View.release(view, tx_key)
      GenServer.cast(db, :try_sweep)
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
  def get(db, key, opts \\ []) do
    read(db, fn tx -> Tx.get(tx, key, opts) end)
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
  def scan(db, opts \\ []) do
    view = ext_ref(db)
    tx_key = make_ref()

    Tx.scan(
      fn ->
        {_max_lk, seq, tx_id} = View.acquire(view, tx_key)
        {seq, View.get_tables(view, tx_id)}
      end,
      Keyword.put(opts, :after, fn ->
        View.release(view, tx_key)
        GenServer.cast(db, :try_sweep)
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
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @doc """
  Starts the database, see `start_link/1` for more details.
  """
  def start(opts) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    GenServer.start(__MODULE__, opts, name: opts[:name])
  end

  @doc """
  Stops the database.
  """
  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(db, reason, timeout)
  end

  @impl GenServer
  def init(args) do
    data_dir = args[:data_dir] || raise ":data_dir required"
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
        div(@default_level_base_size, @default_level_size_multiplier)
      )

    case Manifest.open(data_dir) do
      {:ok, manifest} ->
        {:ok,
         %__MODULE__{
           data_dir: data_dir,
           file_counter: file_counter,
           manifest: manifest,
           view: View.new(),
           opts: opts
         }, {:continue, :recover}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_continue(:recover, state) do
    {manifest_seq, no_files, files} = Manifest.snapshot(state.manifest)
    :atomics.put(state.file_counter, 1, no_files)

    case restore_db(state, files) do
      {:ok, state} ->
        seq = max(manifest_seq, state.mem_table.max_sequence)
        View.update_sequence(state.view, seq)
        publish_ready(state.view)
        {:noreply, state, {:continue, :compact}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_continue(:sweep, state) do
    to_remove = View.sweep(state.view)

    case remove(to_remove) do
      :ok -> {:noreply, state, {:continue, :compact}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_continue(:compact, %{compact_ref: nil} = state) do
    opts =
      Keyword.take(state.opts, [
        :flush_level_file_limit,
        :level_base_size,
        :level_size_multiplier
      ])

    case Levels.next(state.levels, opts) do
      nil ->
        {:noreply, state}

      {:merge, lk, dts, filter_tombstones?, levels} ->
        state = compact(state, lk, dts, filter_tombstones?)
        {:noreply, %{state | levels: levels}}
    end
  end

  def handle_continue(:compact, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:transaction, callback}, _from, state) do
    tx_key = make_ref()
    {max_lk, seq, tx_id} = View.acquire(state.view, tx_key)

    tx = %Tx{
      mode: :write,
      view: state.view,
      tx_id: tx_id,
      sequence: seq,
      max_level_key: max_lk
    }

    result =
      try do
        case callback.(tx) do
          {:commit, tx, reply} ->
            View.release(state.view, tx_key)
            new_seq = tx.sequence
            commits = Enum.reverse(tx.commits)

            with {:ok, state} <- apply_commits(state, commits),
                 :ok <- View.update_sequence(state.view, new_seq) do
              {:ok, reply, state}
            end

          :abort ->
            {:error, :aborted}
        end
      rescue
        _ -> {:error, :aborted}
      catch
        _, _ -> {:error, :aborted}
      end

    View.release(state.view, tx_key)

    case result do
      {:ok, reply, state} -> {:reply, reply, state}
      {:error, :aborted} -> {:reply, {:error, :aborted}, state}
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  def handle_call({:export, export_dir}, _from, state) do
    {_, _, files} = Manifest.snapshot(state.manifest)
    files = Enum.map(files, &elem(&1, 1))
    manifest_file = Manifest.current_file(state.manifest)

    case Export.into_tar(export_dir, [manifest_file | files]) do
      {:ok, exported} -> {:reply, {:ok, exported}, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call(:flushing?, _from, state),
    do: {:reply, MapSet.size(state.flush_refs) != 0, state}

  def handle_call(:compacting?, _from, state),
    do: {:reply, state.compact_ref != nil, state}

  @impl GenServer
  def handle_cast(:try_sweep, state) do
    {:noreply, state, {:continue, :sweep}}
  end

  @impl GenServer
  def handle_info({ref, {:ok, new, old}}, state) do
    if MapSet.member?(state.flush_refs, ref) or ref == state.compact_ref do
      flush_refs = MapSet.delete(state.flush_refs, ref)
      compact_ref = if ref == state.compact_ref, do: nil, else: state.compact_ref

      with {:ok, manifest} <-
             Manifest.update(
               state.manifest,
               tag_id_with_type(new),
               tag_id_with_type(old),
               state.mem_table.max_sequence
             ) do
        state = make_visible(state, new)
        Enum.each(old, &View.soft_delete_table(state.view, &1.id))

        {:noreply,
         %{
           state
           | manifest: manifest,
             flush_refs: flush_refs,
             compact_ref: compact_ref
         }, {:continue, :sweep}}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info({ref, {:error, reason}}, state) do
    if MapSet.member?(state.flush_refs, ref) or ref == state.compact_ref,
      do: {:stop, reason, state},
      else: {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    state.manifest && Manifest.close(state.manifest)
    state.mem_table && MemTable.close(state.mem_table)
    :ok
  end

  defp apply_commits(state, commits) do
    case MemTable.append(state.mem_table, commits) do
      {:ok, mem_table} ->
        {:ok, %{state | mem_table: mem_table}}

      {:ok, old_mt, new_mt} ->
        View.add_table(state.view, new_mt.id, -1, new_mt)
        new = tag_id_with_type([new_mt])
        state = flush(state, old_mt)

        with {:ok, manifest} <- Manifest.update(state.manifest, new, [], old_mt.max_sequence) do
          {:ok, %{state | mem_table: new_mt, manifest: manifest}}
        end

      error ->
        error
    end
  end

  defp restore_db(%{mem_table: nil} = state, []) do
    with {:ok, state} <- open_mem_table(state),
         new = tag_id_with_type([state.mem_table]),
         {:ok, manifest} <- Manifest.update(state.manifest, new, [], state.mem_table.max_sequence) do
      {:ok, %{state | manifest: manifest}}
    end
  end

  defp restore_db(state, []), do: {:ok, state}

  defp restore_db(state, [{:mem, file} | files]) do
    state =
      case state.mem_table do
        nil -> state
        mt -> flush(state, mt)
      end

    with {:ok, state} <- open_mem_table(state, file) do
      restore_db(state, files)
    end
  end

  defp restore_db(state, [{:disk, file} | files]) do
    with {:ok, dt} <- DiskTable.from_file(file) do
      make_visible(state, [dt])
      levels = Levels.put(state.levels, dt.level_key, dt)
      state = %{state | levels: levels}
      restore_db(state, files)
    end
  end

  defp open_mem_table(state, file \\ nil) do
    filer = file_gen(state.file_counter, state.data_dir, @wal_suffix)
    file = file || filer.()

    opts = [
      mem_limit: state.opts[:mem_limit],
      filer: filer
    ]

    with {:ok, mt} <- MemTable.new(file, opts) do
      make_visible(state, [mt])
      {:ok, %{state | mem_table: mt}}
    end
  end

  defp make_visible(state, []), do: state

  defp make_visible(state, [%MemTable{} = mt | rest]) do
    View.add_table(state.view, mt.id, -1, mt)
    make_visible(state, rest)
  end

  defp make_visible(state, [%DiskTable{} = dt | rest]) do
    View.add_table(state.view, dt.id, dt.level_key, dt)
    levels = Levels.put(state.levels, dt.level_key, dt)
    make_visible(%{state | levels: levels}, rest)
  end

  defp flush(state, mt) do
    opts = [
      level_key: 0,
      compress?: false,
      bit_array_size: state.opts[:bit_array_size],
      fpp: state.opts[:fpp],
      max_size: state.opts[:max_sst_size],
      filer: file_gen(state.file_counter, state.data_dir)
    ]

    flush_ref = create_flush_job(mt, opts) |> start_job()
    flush_refs = MapSet.put(state.flush_refs, flush_ref)
    %{state | flush_refs: flush_refs}
  end

  defp compact(state, lk, dts, filter_tombstones?) do
    opts = [
      level_key: lk,
      compress?: lk > 1,
      bit_array_size: state.opts[:bit_array_size],
      max_size: state.opts[:max_sst_size],
      fpp: state.opts[:fpp],
      filer: file_gen(state.file_counter, state.data_dir),
      filter_tombstones?: filter_tombstones?
    ]

    compact_ref = create_compaction_job(dts, opts) |> start_job()
    %{state | compact_ref: compact_ref}
  end

  defp create_flush_job(mt, opts) do
    fn ->
      stream = MemTable.stream(mt)

      with {:ok, dts} <- DiskTable.build(stream, opts) do
        {:ok, dts, [mt]}
      end
    end
  end

  defp create_compaction_job(dts, opts) do
    fn ->
      stream =
        KMerger.k_merge(
          fn -> Enum.map(dts, &DiskTable.stream/1) end,
          filter_tombstones?: opts[:filter_tombstones?]
        )

      with {:ok, new_dts} <- DiskTable.build(stream, opts) do
        {:ok, new_dts, dts}
      end
    end
  end

  defp start_job(job) do
    %{ref: ref} = Task.async(job)
    ref
  end

  defp remove([]), do: :ok

  defp remove([table | rest]) do
    with :ok <- FileIO.remove(table.id) do
      remove(rest)
    end
  end

  defp file_gen(ref, dir, suffix \\ @goblin_suffix) do
    fn ->
      prefix =
        (:atomics.add_get(ref, 1, 1) - 1)
        |> Integer.to_string(16)
        |> String.pad_leading(20, "0")

      Path.join(dir, "#{prefix}.#{suffix}")
    end
  end

  defp tag_id_with_type([]), do: []

  defp tag_id_with_type([table | tables]),
    do: [tag_id_with_type(table) | tag_id_with_type(tables)]

  defp tag_id_with_type(%MemTable{} = mt), do: {:mem, mt.id}
  defp tag_id_with_type(%DiskTable{} = dt), do: {:disk, dt.id}

  defp publish_ready(ref), do: Process.put(:"$goblin_ext_ref", ref)

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
