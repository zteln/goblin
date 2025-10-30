defmodule Goblin do
  @moduledoc """
  An embedded LSM-Tree database for Elixir.

  Goblin provides a persistent key-value store with ACID transactions,
  automatic compaction, and crash recovery. It uses a Log-Structured
  Merge-Tree architecture optimized for write-heavy workloads.

  ## Starting a database

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        db_dir: "/path/to/db",
        key_limit: 50_000,
        level_limit: 128 * 1024 * 1024
      )

  ## Basic operations

      Goblin.put(db, :user_123, %{name: "Alice", age: 30})
      Goblin.get(db, :user_123)
      # => %{name: "Alice", age: 30}

      Goblin.remove(db, :user_123)
      Goblin.get(db, :user_123)
      # => nil

  ## Batch operations

      Goblin.put_multi(db, [
        {:user_1, %{name: "Alice"}},
        {:user_2, %{name: "Bob"}}
      ])

      Goblin.remove_multi(db, [:user_1, :user_2])

  ## Transactions

      alias Goblin.Tx

      Goblin.transaction(db, fn tx ->
        count = Tx.get(tx, :counter) || 0
        tx = Tx.put(tx, :counter, count + 1)
        {:commit, tx, count + 1}
      end)

  Transactions provide snapshot isolation. If a conflict is detected,
  the transaction returns `{:error, :in_conflict}`.

  ## Configuration

  - `name` - Registered name for the database supervisor (optional)
  - `db_dir` - Directory path for storing database files (required)
  - `key_limit` - Maximum keys in MemTable before flushing (default: 50,000)
  - `level_limit` - Size threshold in bytes for level 0 compaction (default: 128 MB)

  ## Architecture

  Goblin consists of several supervised components:

  - **Writer** - Manages in-memory MemTable and coordinates writes
  - **Store** - Tracks SST files and provides read access
  - **Compactor** - Merges SST files across levels
  - **WAL** - Write-ahead log for durability
  - **Manifest** - Tracks database state and file metadata
  - **RWLocks** - Coordinates concurrent access to SST files
  """
  use Supervisor

  @type db_key_limit :: non_neg_integer()
  @type db_level_limit :: non_neg_integer()
  @type db_key :: term()
  @type db_value :: term() | nil
  @type db_level_key :: non_neg_integer()
  @type db_sequence :: non_neg_integer()
  @type triple :: {Goblin.db_sequence(), Goblin.db_key(), Goblin.db_value()}
  @type db_file :: String.t()
  @type db_server :: GenServer.server()

  @default_key_limit 50_000
  @default_level_limit 128 * 1024 * 1024

  @doc """
  Executes a function within a database transaction.

  Transactions provide snapshot isolation and atomic commits. The function
  receives a transaction struct and must return either `{:commit, tx, result}`
  to commit changes or `:cancel` to abort.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `f` - A function that takes a `Goblin.Transaction.t()` and returns a transaction result

  ## Returns

  - `result` - The value returned from the transaction function on successful commit
  - `:ok` - When the transaction is cancelled
  - `{:error, :in_conflict}` - When a write conflict is detected
  - `{:error, term()}` - Other errors

  ## Examples

      Goblin.transaction(db, fn tx ->
        count = Goblin.Tx.get(tx, :counter) || 0
        tx = Goblin.Tx.put(tx, :counter, count + 1)
        {:commit, tx, count + 1}
      end)
      # => 1

      Goblin.transaction(db, fn _tx ->
        :cancel
      end)
      # => :ok
  """
  @spec transaction(db_server(), (Goblin.Transaction.t() -> Goblin.Writer.transaction_return())) ::
          term() | :ok | {:error, term()}
  def transaction(db, f) do
    writer = name(db, :writer)
    Goblin.Writer.transaction(writer, f)
  end

  @doc """
  Writes a key-value pair to the database.

  This operation is atomic and durable. The write is first recorded in the
  write-ahead log, then added to the in-memory MemTable.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to store (will be serialized)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put(db, :user_123, %{name: "Alice", age: 30})
      # => :ok

      Goblin.put(db, "config:timeout", 5000)
      # => :ok
  """
  @spec put(db_server(), db_key(), db_value()) :: :ok
  def put(db, key, value) do
    writer = name(db, :writer)
    Goblin.Writer.put(writer, key, value)
  end

  @doc """
  Writes multiple key-value pairs to the database in a single operation.

  This is more efficient than calling `put/3` multiple times as it batches
  the writes together.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `pairs` - A list of `{key, value}` tuples

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put_multi(db, [
        {:user_1, %{name: "Alice"}},
        {:user_2, %{name: "Bob"}},
        {:user_3, %{name: "Charlie"}}
      ])
      # => :ok
  """
  @spec put_multi(db_server(), [{db_key(), db_value()}]) :: :ok
  def put_multi(db, pairs) do
    writer = name(db, :writer)
    Goblin.Writer.put_multi(writer, pairs)
  end

  @doc """
  Removes a key from the database.

  This operation writes a tombstone marker for the key. The actual data
  is removed during compaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to remove

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove(db, :user_123)
      # => :ok

      Goblin.get(db, :user_123)
      # => nil
  """
  @spec remove(db_server(), db_key()) :: :ok
  def remove(db, key) do
    writer = name(db, :writer)
    Goblin.Writer.remove(writer, key)
  end

  @doc """
  Removes multiple keys from the database in a single operation.

  This is more efficient than calling `remove/2` multiple times as it batches
  the removals together.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to remove

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove_multi(db, [:user_1, :user_2, :user_3])
      # => :ok
  """
  @spec remove_multi(db_server(), [db_key()]) :: :ok
  def remove_multi(db, keys) do
    writer = name(db, :writer)
    Goblin.Writer.remove_multi(writer, keys)
  end

  @doc """
  Retrieves the value associated with a key from the database.

  Searches the MemTable first, then SST files from newest to oldest.
  Returns the default value if the key is not found.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to look up
  - `default` - Value to return if key is not found (default: `nil`)

  ## Returns

  - The value associated with the key, or `default` if not found

  ## Examples

      Goblin.put(db, :user_123, %{name: "Alice"})
      Goblin.get(db, :user_123)
      # => %{name: "Alice"}

      Goblin.get(db, :nonexistent)
      # => nil

      Goblin.get(db, :nonexistent, :not_found)
      # => :not_found
  """
  @spec get(db_server(), db_key(), db_value()) :: db_value()
  def get(db, key, default \\ nil) do
    writer = name(db, :writer)
    store = name(db, :store)

    case Goblin.Reader.get(key, writer, store) do
      :not_found -> default
      {_seq, value} -> value
    end
  end

  @doc """
  Retrieves values for multiple keys from the database in a single operation.

  This is more efficient than calling `get/3` multiple times as it batches
  the reads together. Only returns key-value pairs for keys that exist.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to look up

  ## Returns

  - A list of `{key, value}` tuples for found keys, sorted by key

  ## Examples

      Goblin.put_multi(db, [
        {:user_1, %{name: "Alice"}},
        {:user_2, %{name: "Bob"}}
      ])

      Goblin.get_multi(db, [:user_1, :user_2, :user_3])
      # => [{:user_1, %{name: "Alice"}}, {:user_2, %{name: "Bob"}}]

      Goblin.get_multi(db, [:nonexistent])
      # => []
  """
  @spec get_multi(db_server(), [db_key()]) :: [{db_key(), db_value()}]
  def get_multi(db, keys) when is_list(keys) do
    writer = name(db, :writer)
    store = name(db, :store)

    Goblin.Reader.get_multi(keys, writer, store)
    |> Enum.reject(&(&1 == :not_found))
    |> Enum.map(fn {key, _, value} -> {key, value} end)
    |> List.keysort(0)
  end

  def get_multi(_, _), do: raise("`keys` not a list.")

  @doc """
  Retrieves all key-value pairs within a specified range from the database.

  Returns entries sorted by key in ascending order. Both `min` and `max` are
  inclusive. If neither bound is specified, returns all entries in the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `opts` - Keyword list of options:
    - `:min` - Minimum key (inclusive, optional)
    - `:max` - Maximum key (inclusive, optional)

  ## Returns

  - A stream over the key-value pairs in the range

  ## Examples

      Goblin.put_multi(db, [
        {1, "one"},
        {2, "two"},
        {3, "three"},
        {4, "four"},
        {5, "five"}
      ])

      Goblin.select(db, min: 2, max: 4) |> Enum.to_list()
      # => [{2, "two"}, {3, "three"}, {4, "four"}]

      Goblin.select(db, min: 3) |> Enum.to_list()
      # => [{3, "three"}, {4, "four"}, {5, "five"}]

      Goblin.select(db, max: 2) |> Enum.to_list()
      # => [{1, "one"}, {2, "two"}]

      Goblin.select(db) |> Enum.to_list()
      # => [{1, "one"}, {2, "two"}, {3, "three"}, {4, "four"}, {5, "five"}]
  """
  @spec select(db_server(), keyword()) :: Stream.t()
  def select(db, opts \\ []) do
    writer = name(db, :writer)
    store = name(db, :store)
    min = opts[:min]
    max = opts[:max]

    Goblin.Reader.select(min, max, writer, store)
  end

  # def is_compacting do
  #
  # end
  #
  # def is_flushing do
  #
  # end

  @doc """
  Starts the Goblin database supervisor and all child processes.

  Creates the database directory if it doesn't exist and initializes all
  components including the Writer, Store, Compactor, WAL, and Manifest.

  ## Options

  - `:name` - Registered name for the supervisor (optional)
  - `:db_dir` - Directory path for database files (required)
  - `:key_limit` - Max keys in MemTable before flush (default: 50,000)
  - `:level_limit` - Size threshold for level 0 compaction in bytes (default: 128 MB)

  ## Returns

  - `{:ok, pid}` - On successful start
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        db_dir: "/var/lib/myapp/db",
        key_limit: 100_000,
        level_limit: 256 * 1024 * 1024
      )
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    db_dir = opts[:db_dir] || raise "no db_dir provided."
    File.exists?(db_dir) || File.mkdir_p!(db_dir)
    Supervisor.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  @impl true
  def init(args) do
    db_dir = args[:db_dir]
    key_limit = args[:key_limit] || @default_key_limit
    level_limit = args[:level_limit] || @default_level_limit
    task_sup_name = name(args[:name], :task_sup)
    rw_locks_name = name(args[:name], :rw_locks)
    manifest_name = name(args[:name], :manifest)
    wal_name = name(args[:name], :wal)
    writer_name = name(args[:name], :writer)
    compactor_name = name(args[:name], :compactor)
    store_name = name(args[:name], :store)

    children = [
      {Task.Supervisor, name: task_sup_name},
      {Goblin.RWLocks, Keyword.merge(args, name: rw_locks_name)},
      {Goblin.Manifest, Keyword.merge(args, name: manifest_name, db_dir: db_dir)},
      {Goblin.WAL, Keyword.merge(args, name: wal_name, db_dir: db_dir)},
      {Goblin.Compactor,
       Keyword.merge(args,
         name: compactor_name,
         key_limit: key_limit,
         level_limit: level_limit,
         store: store_name,
         rw_locks: rw_locks_name,
         level_limit: level_limit,
         manifest: manifest_name,
         task_sup: task_sup_name
       )},
      {Goblin.Store,
       Keyword.merge(args,
         name: store_name,
         dir: db_dir,
         writer: writer_name,
         rw_locks: rw_locks_name,
         compactor: compactor_name,
         manifest: manifest_name
       )},
      {Goblin.Writer,
       Keyword.merge(args,
         name: writer_name,
         key_limit: key_limit,
         store: store_name,
         wal: wal_name,
         manifest: manifest_name,
         task_sup: task_sup_name
       )}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp name(pid, suffix) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, []} ->
        name(nil, suffix)

      {:registered_name, __MODULE__} ->
        name(nil, suffix)

      {:registered_name, registered_name} ->
        name(registered_name, suffix)
    end
  end

  defp name(name, suffix) do
    if name, do: :"#{name}_#{suffix}", else: :"goblin_#{suffix}"
  end
end
