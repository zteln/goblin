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
    :opts,
    :sequence,
    levels: %{},
    flushing: %{},
    compacting: %{},
    readers: %{}
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
    view = get_view(db)
    tx_key = make_ref()
    GenServer.cast(db, {:track_reader, self(), tx_key})

    try do
      {seq, max_lk, tx_id} = View.add_reader(view, tx_key)

      %Tx{
        mode: :read,
        view: view,
        tx_id: tx_id,
        sequence: seq,
        max_level_key: max_lk
      }
      |> callback.()
    after
      GenServer.cast(db, {:untrack_reader, tx_key})
      View.release_reader(view, tx_key)
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
  Captures snapshots at enumeration.

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

      scan = Goblin.scan(db)
      Enum.to_list(scan)
      # => []
      Goblin.put(db, :alice, "Alice")
      Enum.to_list(scan)
      # => [{:alice, "Alice"}]
  """
  def scan(db, opts \\ []) do
    view = get_view(db)
    tx_key = make_ref()

    Tx.scan(
      fn ->
        GenServer.cast(db, {:track_reader, self(), tx_key})
        {seq, _max_lk, tx_id} = View.add_reader(view, tx_key)
        {seq, View.get_tables(view, tx_id)}
      end,
      Keyword.put(opts, :after, fn ->
        GenServer.cast(db, {:untrack_reader, tx_key})
        View.release_reader(view, tx_key)
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
    {manifest_seq, no_files, files, dirt} = Manifest.snapshot(state.manifest)
    :atomics.put(state.file_counter, 1, no_files)

    with {:ok, orphans} <- clear_tables(dirt),
         {:ok, state} <- restore_db(state, files),
         {:ok, manifest} <- Manifest.sweep_dirt(state.manifest, orphans) do
      publish_tables(state)
      seq = max(manifest_seq, state.sequence)
      View.put_sequence(state.view, seq)
      mark_ready(self(), state.view)
      {:noreply, %{state | manifest: manifest, sequence: seq}, {:continue, :compact}}
    else
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_continue(:flush, state) do
    if MemTable.size(state.mem_table) > state.opts[:mem_limit] do
      filer = file_gen(state.file_counter, state.data_dir, @wal_suffix)

      with {:ok, new_mt} <- MemTable.rotate(state.mem_table, filer: filer),
           {:ok, manifest} <-
             Manifest.update(
               state.manifest,
               tag_id_with_type([new_mt]),
               [],
               state.sequence
             ) do
        state =
          %{state | mem_table: new_mt, manifest: manifest}
          |> merge(0, [state.mem_table])

        publish_tables(state)
        {:noreply, state, {:continue, :compact}}
      else
        {:error, reason} -> {:stop, reason, state}
      end
    else
      {:noreply, state, {:continue, :compact}}
    end
  end

  def handle_continue(:compact, %{compacting: compacting} = state)
      when map_size(compacting) == 0 do
    opts =
      Keyword.take(state.opts, [
        :mem_limit,
        :flush_level_file_limit,
        :level_base_size,
        :level_size_multiplier
      ])

    case Levels.next(state.levels, opts) do
      nil ->
        {:noreply, state}

      {:merge, lk, dts, filter_tombstones?, levels} ->
        state =
          %{state | levels: levels}
          |> merge(lk, dts, filter_tombstones?)

        {:noreply, state}
    end
  end

  def handle_continue(:compact, state), do: {:noreply, state}

  def handle_continue(:sweep, state) do
    case View.sweep(state.view) do
      [] ->
        {:noreply, state, {:continue, :compact}}

      to_remove ->
        with {:ok, paths} <- clear_tables(to_remove),
             {:ok, manifest} <- Manifest.sweep_dirt(state.manifest, paths) do
          {:noreply, %{state | manifest: manifest}, {:continue, :compact}}
        else
          {:error, reason} -> {:stop, reason, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:transaction, callback}, _from, state) do
    tx_key = make_ref()
    {seq, max_lk, tx_id} = View.add_reader(state.view, tx_key)

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
            new_seq = tx.sequence
            commits = Enum.reverse(tx.commits)

            with {:ok, mem_table} <- MemTable.append(state.mem_table, commits) do
              View.put_sequence(state.view, new_seq)
              {:ok, reply, %{state | mem_table: mem_table, sequence: new_seq}}
            end

          :abort ->
            {:error, :aborted}
        end
      rescue
        _ -> {:error, :aborted}
      catch
        _, _ -> {:error, :aborted}
      end

    View.release_reader(state.view, tx_key)

    case result do
      {:ok, reply, state} -> {:reply, reply, state, {:continue, :flush}}
      {:error, :aborted} -> {:reply, {:error, :aborted}, state}
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  def handle_call({:export, export_dir}, _from, state) do
    {_, _, files, _} = Manifest.snapshot(state.manifest)
    files = Enum.map(files, &elem(&1, 1))
    manifest_file = Manifest.current_file(state.manifest)

    case Export.into_tar(export_dir, [manifest_file | files]) do
      {:ok, exported} -> {:reply, {:ok, exported}, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call(:flushing?, _from, state),
    do: {:reply, map_size(state.flushing) != 0, state}

  def handle_call(:compacting?, _from, state),
    do: {:reply, map_size(state.compacting) != 0, state}

  @impl GenServer
  def handle_cast(:try_sweep, state) do
    {:noreply, state, {:continue, :sweep}}
  end

  def handle_cast({:track_reader, reader_pid, reader_key}, state) do
    monitor_ref = Process.monitor(reader_pid)
    readers = Map.put(state.readers, reader_key, monitor_ref)
    {:noreply, %{state | readers: readers}}
  end

  def handle_cast({:untrack_reader, reader_key}, state) do
    {monitor_ref, readers} = Map.pop(state.readers, reader_key)
    monitor_ref && Process.demonitor(monitor_ref, [:flush])
    {:noreply, %{state | readers: readers}}
  end

  @impl GenServer
  def handle_info({ref, {:ok, new, old}}, state) do
    if Map.has_key?(state.flushing, ref) or Map.has_key?(state.compacting, ref) do
      flushing = Map.delete(state.flushing, ref)
      compacting = Map.delete(state.compacting, ref)

      with {:ok, manifest} <-
             Manifest.update(
               state.manifest,
               tag_id_with_type(new),
               tag_id_with_type(old),
               state.sequence
             ) do
        levels = Enum.reduce(new, state.levels, &Levels.put(&2, &1))

        state = %{
          state
          | levels: levels,
            manifest: manifest,
            flushing: flushing,
            compacting: compacting
        }

        publish_tables(state)

        {:noreply, state, {:continue, :sweep}}
      else
        {:error, reason} -> {:stop, reason, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info({ref, {:error, reason}}, state) do
    if Map.has_key?(state.flushing, ref) or Map.has_key?(state.compacting, ref),
      do: {:stop, reason, state},
      else: {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, _, _, reason},
        %{flushing: flushing, compacting: compacting} = state
      )
      when is_map_key(flushing, ref) or is_map_key(compacting, ref) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    case Enum.find(state.readers, fn {_, monitor_ref} -> monitor_ref == ref end) do
      nil ->
        {:noreply, state}

      {reader_key, _} ->
        View.release_reader(state.view, reader_key)
        readers = Map.delete(state.readers, reader_key)
        {:noreply, %{state | readers: readers}}
    end
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    state.manifest && Manifest.close(state.manifest)
    state.mem_table && MemTable.close(state.mem_table)
    :ok
  end

  defp restore_db(%{mem_table: nil} = state, []) do
    with {:ok, state} <- open_mem_table(state),
         new = tag_id_with_type([state.mem_table]),
         {:ok, manifest} <- Manifest.update(state.manifest, new, [], state.sequence) do
      {:ok, %{state | manifest: manifest}}
    end
  end

  defp restore_db(state, []), do: {:ok, state}

  defp restore_db(state, [{:mem, file} | files]) do
    state =
      case state.mem_table do
        nil -> state
        mt -> merge(state, 0, [mt])
      end

    with {:ok, state} <- open_mem_table(state, file) do
      restore_db(state, files)
    end
  end

  defp restore_db(state, [{:disk, file} | files]) do
    with {:ok, dt} <- DiskTable.from_file(file) do
      levels = Levels.put(state.levels, dt)
      state = %{state | levels: levels}
      restore_db(state, files)
    end
  end

  defp open_mem_table(state, file \\ nil) do
    filer = file_gen(state.file_counter, state.data_dir, @wal_suffix)
    file = file || filer.()

    with {:ok, seq, mt} <- MemTable.new(file) do
      {:ok, %{state | mem_table: mt, sequence: seq}}
    end
  end

  defp publish_tables(state) do
    mem_tables = [state.mem_table | Map.values(state.flushing)]

    levels =
      state.compacting
      |> Map.values()
      |> List.flatten()
      |> Enum.reduce(state.levels, &Levels.put(&2, &1))

    View.put_snapshot(state.view, mem_tables, levels)
  end

  defp merge(state, lk, tables, filter_tombstones? \\ false) do
    opts = [
      level_key: lk,
      compress?: lk > 1,
      bit_array_size: state.opts[:bit_array_size],
      max_size: state.opts[:max_sst_size],
      fpp: state.opts[:fpp],
      filer: file_gen(state.file_counter, state.data_dir),
      filter_tombstones?: filter_tombstones?
    ]

    run_merge(state, tables, opts)
  end

  defp run_merge(state, [%MemTable{} = mt], opts) do
    %{ref: ref} =
      Task.async(fn ->
        stream = MemTable.stream(mt)

        with {:ok, dts} <- DiskTable.build(stream, opts) do
          {:ok, dts, [mt]}
        end
      end)

    flushing = Map.put(state.flushing, ref, mt)
    %{state | flushing: flushing}
  end

  defp run_merge(state, dts, opts) do
    %{ref: ref} =
      Task.async(fn ->
        stream =
          KMerger.k_merge(
            fn -> Enum.map(dts, &DiskTable.stream/1) end,
            filter_tombstones?: opts[:filter_tombstones?]
          )

        with {:ok, new_dts} <- DiskTable.build(stream, opts) do
          {:ok, new_dts, dts}
        end
      end)

    compacting = Map.put(state.compacting, ref, dts)
    %{state | compacting: compacting}
  end

  defp clear_tables(tables, acc \\ [])
  defp clear_tables([], acc), do: {:ok, acc}

  defp clear_tables([%MemTable{} = mt | rest], acc) do
    with :ok <- FileIO.remove(mt.id) do
      MemTable.destroy(mt)
      clear_tables(rest, [mt.id | acc])
    end
  end

  defp clear_tables([%DiskTable{} = dt | rest], acc) do
    with :ok <- FileIO.remove(dt.id) do
      clear_tables(rest, [dt.id | acc])
    end
  end

  defp clear_tables([path | rest], acc) do
    with :ok <- FileIO.remove(path) do
      clear_tables(rest, [path | acc])
    end
  end

  defp file_gen(ref, dir, suffix \\ @goblin_suffix) do
    fn ->
      prefix =
        (:atomics.add_get(ref, 1, 1) - 1)
        |> Integer.to_string(16)
        |> String.pad_leading(20, "0")

      path = Path.join(dir, "#{prefix}.#{suffix}")
      if File.exists?(path), do: File.rm!(path)
      path
    end
  end

  defp tag_id_with_type([]), do: []

  defp tag_id_with_type([table | tables]),
    do: [tag_id_with_type(table) | tag_id_with_type(tables)]

  defp tag_id_with_type(%MemTable{} = mt), do: {:mem, mt.id}
  defp tag_id_with_type(%DiskTable{} = dt), do: {:disk, dt.id}

  defp mark_ready(db, ref), do: :persistent_term.put({__MODULE__, db}, ref)

  defp get_view(db, timeout \\ 20_000)
  defp get_view(_, timeout) when timeout <= 0, do: raise("db not ready")

  defp get_view(db, timeout) do
    case :persistent_term.get({__MODULE__, db}, nil) do
      nil ->
        Process.sleep(50)
        get_view(db, timeout - 50)

      view ->
        view
    end
  end
end
