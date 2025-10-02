defmodule SeaGoat.Store do
  @moduledoc """
  A GenServer that manages SSTable file storage and organization for the SeaGoat database.

  The Store is responsible for:
  - Organizing SSTable files by levels in an LSM-tree structure
  - Managing bloom filters for efficient key lookup
  - Coordinating with the compactor for file reorganization
  - Handling file lifecycle including creation, removal, and reuse during compaction
  - Providing thread-safe access to SSTable files through read/write locks
  - Persisting and replaying state for crash recovery

  The Store maintains a hierarchical level structure where:
  - Level 0 contains the newest data (from memtable flushes)
  - Higher levels contain progressively older, more compacted data
  - Each file is associated with a bloom filter for fast key existence checks

  ## State Recovery

  On startup, the Store scans its directory for existing SSTable files and WAL entries,
  replaying them to reconstruct its internal state. This ensures data durability across
  process restarts.

  ## Concurrency

  The Store coordinates with several other processes:
  - `SeaGoat.Writer` for memtable flushes
  - `SeaGoat.Compactor` for background compaction
  - `SeaGoat.WAL` for write-ahead logging
  - `SeaGoat.RWLocks` for coordinated file access
  """
  use GenServer
  alias __MODULE__.Levels
  alias SeaGoat.Compactor
  alias SeaGoat.SSTables
  alias SeaGoat.WAL
  alias SeaGoat.RWLocks
  alias SeaGoat.BloomFilter

  require SeaGoat.Writer
  require SeaGoat.Compactor

  @file_suffix ".seagoat"
  @tmp_suffix ".tmp"
  @dump_suffix ".dump"

  @type file :: String.t()
  @type level :: non_neg_integer()

  defstruct [
    :dir,
    :wal,
    :compactor,
    :rw_locks,
    :latest_wal,
    compacting_files: %{},
    writes: [],
    file_count: 0,
    levels: Levels.new()
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :wal, :compactor, :rw_locks])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @doc """
  Adds an SSTable file with its bloom filter to the specified level.

  This function stores the file and its associated bloom filter in the level hierarchy
  and notifies the compactor about the new file for potential compaction.

  ## Parameters
  - `store` - The Store GenServer process
  - `file` - Path to the SSTable file
  - `bloom_filter` - Bloom filter for efficient key existence checks
  - `level` - The level where this file should be placed (0 = newest data)

  ## Examples

      iex> Store.put(store, "/data/1.seagoat", bloom_filter, 0)
      :ok
  """
  @spec put(GenServer.server(), SeaGoat.db_file(), BloomFilter.t(), SeaGoat.db_level()) :: :ok
  def put(store, file, bloom_filter, level) do
    GenServer.call(store, {:put, file, bloom_filter, level})
  end

  @doc """
  Removes one or more SSTable files from the specified level.

  This is typically called during compaction when files are merged into higher levels.
  The function safely handles removal of non-existent files.

  ## Parameters
  - `store` - The Store GenServer process  
  - `files` - List of file paths to remove
  - `level` - The level from which to remove the files

  ## Examples

      iex> Store.remove(store, ["/data/1.seagoat", "/data/2.seagoat"], 0)
      :ok
  """
  @spec remove(GenServer.server(), [SeaGoat.db_file()], SeaGoat.db_level()) :: :ok
  def remove(store, files, level) do
    GenServer.call(store, {:remove, files, level})
  end

  @doc """
  Generates a new unique filename for an SSTable file.

  Creates sequential filenames in the format `{counter}.seagoat` within the Store's
  configured directory. The counter is automatically incremented for each new file.

  ## Parameters
  - `store` - The Store GenServer process

  ## Returns
  A string path to the new file

  ## Examples

      iex> Store.new_file(store)
      "/tmp/seagoat_data/1.seagoat"
      
      iex> Store.new_file(store)
      "/tmp/seagoat_data/2.seagoat"
  """
  @spec new_file(GenServer.server()) :: SeaGoat.db_file()
  def new_file(store) do
    GenServer.call(store, :new_file)
  end

  @doc """
  Retrieves and removes a file from the compacting files registry.

  During compaction, files may be temporarily registered for reuse. This function
  retrieves the registered file and removes it from the registry.

  ## Parameters
  - `store` - The Store GenServer process
  - `files` - List of file paths used as the registry key

  ## Returns
  The registered file path, or `nil` if no file was registered for the given key

  ## Examples

      iex> Store.reuse_file(store, ["old1.seagoat", "old2.seagoat"])
      "/tmp/compacted.seagoat"
  """
  @spec reuse_file(GenServer.server(), [SeaGoat.db_file()]) :: SeaGoat.db_file()
  def reuse_file(store, files) do
    GenServer.call(store, {:reuse_file, files})
  end

  @doc """
  Retrieves SSTable access functions for files that might contain the given key.

  Uses bloom filters to efficiently identify which SSTable files might contain the key,
  then returns functions to read from those files. The read operations are protected
  by read/write locks to ensure thread safety.

  ## Parameters
  - `store` - The Store GenServer process
  - `key` - The key to search for

  ## Returns
  A list of tuples `{read_function, unlock_function}` where:
  - `read_function` - A zero-arity function that reads the key from the SSTable
  - `unlock_function` - A zero-arity function that releases the read lock

  ## Examples

      iex> ss_tables = Store.get_ss_tables(store, "my_key")
      iex> [{read_fn, unlock_fn}] = ss_tables
      iex> value = read_fn.()
      iex> unlock_fn.()

  ## Important
  Always call the unlock function after reading to release the lock, preferably
  in a try/after block to ensure cleanup even if reading fails.
  """
  @spec get_ss_tables(GenServer.server(), SeaGoat.db_key()) :: [
          {(-> SeaGoat.db_value()), (-> :ok)}
        ]
  def get_ss_tables(store, key) do
    GenServer.call(store, {:get_ss_tables, key})
  end

  def get_write_replays(store) do
    GenServer.call(store, :get_write_replays)
  end

  @doc """
  Returns a temporary filename by appending the `.tmp` suffix.

  Utility function for creating temporary file paths, typically used during
  file operations that require atomic writes.

  ## Parameters
  - `file` - The base filename

  ## Returns
  The filename with `.tmp` suffix appended

  ## Examples

      iex> Store.tmp_file("data.seagoat")
      "data.seagoat.tmp"
  """
  @spec tmp_file(SeaGoat.db_file()) :: SeaGoat.db_file()
  def tmp_file(file), do: file <> @tmp_suffix

  @doc """
  Returns a dump filename by appending the `.dump` suffix.

  Utility function for creating dump file paths, typically used for
  debugging or backup operations.

  ## Parameters  
  - `file` - The base filename

  ## Returns
  The filename with `.dump` suffix appended

  ## Examples

      iex> Store.dump_file("data.seagoat")
      "data.seagoat.dump"
  """
  @spec dump_file(SeaGoat.db_file()) :: SeaGoat.db_file()
  def dump_file(file), do: file <> @dump_suffix

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       dir: args[:dir],
       wal: args[:wal],
       rw_locks: args[:rw_locks],
       compactor: args[:compactor]
     }, {:continue, :get_previous_state}}
  end

  @impl GenServer
  def handle_call({:put, file, bloom_filter, level}, _from, state) do
    levels = Levels.insert(state.levels, level, {file, bloom_filter})
    {:reply, :ok, %{state | levels: levels}, {:continue, {:put_in_compactor, level, file}}}
  end

  def handle_call({:remove, files, level}, _from, state) do
    levels =
      Levels.remove(state.levels, level, fn {file, _} ->
        file in files
      end)

    {:reply, :ok, %{state | levels: levels}}
  end

  def handle_call({:get_ss_tables, key}, {pid, _ref}, state) do
    ss_tables =
      state.levels
      |> Levels.levels()
      |> Enum.reduce([], fn level, acc ->
        acc ++
          Levels.get_all_entries(
            state.levels,
            level,
            fn {_file, bloom_filter} ->
              BloomFilter.is_member(bloom_filter, key)
            end,
            fn {file, _bloom_filter} ->
              RWLocks.rlock(state.rw_locks, file, pid)

              {fn -> SSTables.read(file, key) end,
               fn -> RWLocks.unlock(state.rw_locks, file, pid) end}
            end
          )
      end)

    {:reply, ss_tables, state}
  end

  def handle_call(:get_write_replays, _from, state) do
    file = file(state.dir, state.latest_wal)
    {:reply, {state.writes, file}, %{state | writes: [], latest_wal: nil}}
  end

  def handle_call(:new_file, _from, state) do
    new_file_count = state.file_count + 1
    new_file = file(state.dir, new_file_count)
    {:reply, new_file, %{state | file_count: new_file_count}}
  end

  def handle_call({:reuse_file, files}, _from, state) do
    {file, compacting_files} = Map.pop(state.compacting_files, files)
    {:reply, file, %{state | compacting_files: compacting_files}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, level, file}, state) do
    file_count = file_count(file)
    :ok = Compactor.put(state.compactor, level, file_count, file)
    {:noreply, state}
  end

  def handle_continue(:get_previous_state, state) do
    state =
      state.dir
      |> File.ls!()
      |> read_previous_state(state)

    {:noreply, state}
  end

  defp read_previous_state([], state), do: %{state | file_count: 0, latest_wal: 0}

  defp read_previous_state(files, state) do
    files
    |> Enum.map(&Path.join(state.dir, &1))
    |> Enum.filter(&valid_db_file?/1)
    |> Enum.flat_map(&tag_file(&1, state.wal))
    |> Enum.sort_by(& &1.file_count)
    |> Enum.reduce(state, &process_file/2)
  end

  defp valid_db_file?(file), do: String.ends_with?(file, [@file_suffix, @dump_suffix])

  defp tag_file(file, wal) do
    [file_count, _ext] =
      file
      |> Path.basename()
      |> String.split(".", parts: 2)

    file_count = String.to_integer(file_count)

    case wal_or_ss_table(file, wal) do
      {:logs, logs} ->
        [%{type: :log, logs: logs, file: file, file_count: file_count}]

      {:ss_table, bloom_filter, level} ->
        [
          %{
            type: :ss_table,
            bloom_filter: bloom_filter,
            level: level,
            file: file,
            file_count: file_count
          }
        ]

      _ ->
        []
    end
  end

  defp process_file(%{type: :log} = file, state) do
    case file.logs do
      [SeaGoat.Writer.writer_tag() | _logs] ->
        writes = [{file.file, file.logs} | state.writes]
        %{state | writes: writes, file_count: file.file_count, latest_wal: file.file_count}

      [{SeaGoat.Compactor.compactor_tag(), files} | logs] ->
        compacting_files = Map.put(state.compacting_files, files, file.file)
        :ok = run_logs(logs)
        %{state | file_count: file.file_count, compacting_files: compacting_files}

      logs ->
        :ok = run_logs(logs)
        %{state | file_count: file.file_count}
    end
  end

  defp process_file(%{type: :ss_table} = file, state) do
    levels = Levels.insert(state.levels, file.level, {file.file, file.bloom_filter})
    :ok = Compactor.put(state.compactor, file.level, file.file_count, file.file)
    %{state | levels: levels, file_count: file.file_count}
  end

  defp run_logs([]), do: :ok

  defp run_logs([{:del, files} | logs]) do
    with :ok <- SSTables.delete(files) do
      run_logs(logs)
    end
  end

  defp run_logs([_ | logs]), do: run_logs(logs)

  defp wal_or_ss_table(file, wal) do
    case WAL.get_logs(wal, file) do
      {:ok, logs} ->
        {:logs, logs}

      _ ->
        case SSTables.fetch_ss_table_info(file) do
          {:ok, bloom_filter, level} ->
            {:ss_table, bloom_filter, level}

          _ ->
            {:error, :not_wal_or_db}
        end
    end
  end

  defp file_count(file) do
    Path.basename(file, @file_suffix)
    |> String.to_integer()
  end

  defp file(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
