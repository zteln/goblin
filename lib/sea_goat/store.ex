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
  alias __MODULE__.FileManager
  alias __MODULE__.Files
  alias __MODULE__.Recovery
  alias SeaGoat.Compactor

  @type file :: String.t()
  @type level :: non_neg_integer()

  defstruct [
    :dir,
    :wal,
    :compactor,
    :rw_locks,
    :latest_wal,
    recovered_compacting_files: %{},
    recovered_writes: [],
    max_file_count: 0,
    max_wal_count: 0,
    files: %{}
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
  @spec put(GenServer.server(), SeaGoat.db_file(), SeaGoat.BloomFilter.t(), SeaGoat.db_level()) ::
          :ok
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
  @spec remove(GenServer.server(), [SeaGoat.db_file()]) :: :ok
  def remove(store, files) do
    GenServer.call(store, {:remove, files})
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

  def get_recovered_writes(store) do
    GenServer.call(store, :get_recovered_writes)
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
  def tmp_file(file), do: FileManager.tmp_file(file)

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
  def dump_file(file), do: FileManager.dump_file(file)

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       dir: args[:dir],
       wal: args[:wal],
       rw_locks: args[:rw_locks],
       compactor: args[:compactor]
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:put, file, bloom_filter, level}, _from, state) do
    files = Files.insert(state.files, file, bloom_filter)
    {:reply, :ok, %{state | files: files}, {:continue, {:put_in_compactor, level, file}}}
  end

  def handle_call({:remove, files}, _from, state) do
    files = Enum.reduce(files, state.files, &Files.remove(&2, &1))
    {:reply, :ok, %{state | files: files}}
  end

  def handle_call({:get_ss_tables, key}, {pid, _ref}, state) do
    ss_tables = Files.get_all(state.files, key, pid, state.rw_locks)
    {:reply, ss_tables, state}
  end

  def handle_call(:get_recovered_writes, _from, state) do
    file = FileManager.file_path(state.dir, state.max_wal_count)
    {:reply, {state.recovered_writes, file}, %{state | recovered_writes: [], max_wal_count: nil}}
  end

  def handle_call(:new_file, _from, state) do
    new_file_count = state.max_file_count + 1
    new_file = FileManager.file_path(state.dir, new_file_count)
    {:reply, new_file, %{state | max_file_count: new_file_count}}
  end

  def handle_call({:reuse_file, files}, _from, state) do
    {file, recovered_compacting_files} = Map.pop(state.recovered_compacting_files, files)
    {:reply, file, %{state | recovered_compacting_files: recovered_compacting_files}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, level, file}, state) do
    file_count = FileManager.file_count_from_path(file)
    :ok = Compactor.put(state.compactor, level, file_count, file)
    {:noreply, state}
  end

  def handle_continue(:recover_state, state) do
    %{
      files: files,
      recovered_writes: recovered_writes,
      recovered_compacting_files: recovered_compacting_files,
      max_file_count: max_file_count,
      max_wal_count: max_wal_count
    } = Recovery.recover_state(state.dir, state.wal, state.compactor)

    state = %{
      state
      | recovered_writes: recovered_writes,
        recovered_compacting_files: recovered_compacting_files,
        files: files,
        max_file_count: max_file_count,
        max_wal_count: max_wal_count
    }

    {:noreply, state}
  end
end
