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
        count = Tx.get(tx, :counter, 0) 
        tx = Tx.put(tx, :counter, count + 1)
        {:commit, tx, count + 1}
      end)

      Goblin.transaction(db, fn tx ->
        key1 = Tx.get(tx, :start, 0)
        key2 = Tx.get(tx, key1)
        {key1, key2}
      end, read_only: true)

  ## Configuration

  - `name` - Registered name for the database supervisor (optional)
  - `db_dir` - Directory path for storing database files (required)
  - `key_limit` - Maximum keys in MemTable before flushing (default: 50,000)
  - `level_limit` - Size threshold in bytes for level 0 compaction (default: 128 MB)

  ## Architecture

  Goblin consists of several supervised components:

  - **Writer** - Manages in-memory MemTable and coordinates writes
  - **Reader** - Keeps track of active readers
  - **Store** - Tracks SST files and provides read access
  - **Compactor** - Merges SST files across levels
  - **WAL** - Write-ahead log for durability
  - **Manifest** - Tracks database state and file metadata
  """
  use Supervisor
  import Goblin.ProcessRegistry

  @typedoc false
  @type db_level_key :: non_neg_integer()
  @typedoc false
  @type seq_no :: non_neg_integer()
  @typedoc false
  @type db_file :: String.t()
  @type db_key_limit :: non_neg_integer()
  @type db_level_limit :: non_neg_integer()
  @type db_key :: term()
  @type db_value :: term()
  @type pair :: {Goblin.db_key(), Goblin.db_value()}
  @type triple :: {Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()}
  @type db_server :: Supervisor.supervisor()
  @type tx_return :: {:commit, Goblin.Tx.t(), term()} | :cancel

  @doc """
  Executes a function within a database transaction.

  There are two different kinds of transactions; read and write. 
  Read transactions cannot write to the database and are executed concurrently.
  Write transactions can write to the database and are executed serially.

  Read transactions read a snapshot of the database from when it starts. 
  A read transaction returns the last evaluation in the transaction function.
  A read transaction that tries to write raises.
  While there are active readers then no files will be cleaned up after compaction.

  Write transactions are executed serially and have atomic commits. The function
  receives a transaction struct and must return either `{:commit, tx, result}`
  to commit changes or `:cancel` to abort.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `f` - A function that takes a `Goblin.Tx.t()` and returns a transaction result
  - `opts` - A keyword list with options to the transaction. Available options:
    - `read_only` - A boolean indicating whether the transaction is read only or not

  ## Returns

  - `result` - The value returned from the transaction function on successful commit
  - `:ok` - When the transaction is cancelled
  - `{:error, :tx_not_found}` - If a transaction was not found for the current process
  - `{:error, :already_in_tx}` - If the current process is already in a transaction
  - `{:error, :not_tx_holder}` - If the process is not the current writer
  - `{:error, term()}` - Other errors

  ## Examples

      # Write transactions:
      Goblin.transaction(db, fn tx ->
        count = Goblin.Tx.get(tx, :counter, 0)
        tx = Goblin.Tx.put(tx, :counter, count + 1)
        {:commit, tx, count + 1}
      end)
      # => 1

      Goblin.transaction(db, fn _tx ->
        :cancel
      end)
      # => :ok

      # Read transactions:
      Goblin.put(db, key, value1)

      Goblin.transaction(db, fn tx -> 
        ^value1 = Goblin.Tx.get(tx, key)

        # Concurrently, in another process
        # Goblin.put(db, key, value2)

        ^value1 = Goblin.Tx.get(tx, key)
      end, read_only: true)
      # => value1

      ^value2 = Goblin.Tx.get(tx, key)
  """
  @spec transaction(db_server(), (Goblin.Tx.t() -> tx_return()), keyword()) ::
          term()
          | :ok
          | {:error, :not_tx_holder | :tx_not_found | :already_in_tx | term()}
  def transaction(db, f, opts \\ []) do
    if Keyword.get(opts, :read_only, false) do
      registry = name(db, ProcessRegistry)
      writer = name(db, Writer)
      store = name(db, Store)
      reader = name(db, Reader)
      Goblin.Reader.transaction(writer, store, reader, via(registry, reader), f)
    else
      registry = name(db, ProcessRegistry)
      writer = name(db, Writer)
      Goblin.Writer.transaction(via(registry, writer), f)
    end
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
    # registry = name(db, ProcessRegistry)
    # writer = name(db, Writer)

    Goblin.transaction(db, fn tx ->
      tx = Goblin.Tx.put(tx, key, value)
      {:commit, tx, :ok}
    end)
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
    # registry = name(db, ProcessRegistry)
    # writer = name(db, Writer)

    Goblin.transaction(db, fn tx ->
      tx =
        Enum.reduce(pairs, tx, fn {k, v}, acc ->
          Goblin.Tx.put(acc, k, v)
        end)

      {:commit, tx, :ok}
    end)
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
    # registry = name(db, ProcessRegistry)
    # writer = name(db, Writer)

    Goblin.transaction(db, fn tx ->
      tx = Goblin.Tx.remove(tx, key)
      {:commit, tx, :ok}
    end)
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
    # registry = name(db, ProcessRegistry)
    # writer = name(db, Writer)

    Goblin.transaction(db, fn tx ->
      tx =
        Enum.reduce(keys, tx, fn key, acc ->
          Goblin.Tx.remove(acc, key)
        end)

      {:commit, tx, :ok}
    end)
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
    writer = name(db, Writer)
    store = name(db, Store)

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
    writer = name(db, Writer)
    store = name(db, Store)

    Goblin.Reader.get_multi(keys, writer, store)
    |> Enum.map(fn {key, _seq, value} -> {key, value} end)
    |> List.keysort(0)
  end

  @doc """
  Retrieves all key-value pairs within a specified range from the database.

  Returns entries sorted by key in ascending order. Both `min` and `max` are
  inclusive. If neither bound is specified, then all entries in the database are returned.
  Specifying only `min` returns all entries from `min` (inclusive) and onwards.
  Specifying only `max` returns all entries from the smallest key until `max` (inclusive).

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
  @spec select(db_server(), keyword()) :: Enumerable.t()
  def select(db, opts \\ []) do
    min = opts[:min]
    max = opts[:max]
    writer = name(db, Writer)
    store = name(db, Store)
    Goblin.Reader.select(min, max, writer, store)
  end

  @doc """
  Checks whether the database is currently compacting or not.

  ## Parameters

  - `db` - The dabase server (PID or registered name)

  ## Returns

  - A boolean indicating whether the database is compacting or not
  """
  @spec is_compacting(db_server()) :: boolean()
  def is_compacting(db) do
    registry = name(db, ProcessRegistry)
    compactor = name(db, Compactor)
    Goblin.Compactor.is_compacting(via(registry, compactor))
  end

  @doc """
  Checks whether the database is currently flushing or not.

  ## Parameters

  - `db` - The database server (PID or registered name)

  ## Returns

  - A boolean indicating whether the database is flushing or not
  """
  @spec is_flushing(db_server()) :: boolean()
  def is_flushing(db) do
    registry = name(db, ProcessRegistry)
    writer = name(db, Writer)
    Goblin.Writer.is_flushing(via(registry, writer))
  end

  @doc """
  Subscribes the current process for database writes. 
  When a write occurs then either `{:put, key, value}` or `{:remove, key}` is dispatched to the subscribers mailbox.
  Batch writes result in separate messages.

  ## Parameters

  - `db` - The database server (pid or registered name)

  ## Returns

  - `:ok` if successful, `{:error, reason}` otherwise

  ## Examples

      Goblin.subscribe(db)
      # => :ok

      Goblin.put(db, :user_1, %{name: "Alice"})
      
      flush()
      # => {:put, :user_1, %{name: "Alice"}}

      Goblin.remove(db, :user_1)
      
      flush()
      # => {:remove, :user_1}
  """
  @spec subscribe(db_server()) :: :ok | {:error, term()}
  def subscribe(db) do
    pub_sub = name(db, PubSub)

    with {:ok, _} <- Goblin.PubSub.subscribe(pub_sub) do
      :ok
    end
  end

  @doc """
  Unsubscribes the current process for future database writes.

  ## Parameters

  - `db` - The database server (pid or registered name)

  ## Returns

  - `:ok`
  """
  @spec unsubscribe(db_server()) :: :ok | {:error, term()}
  def unsubscribe(db) do
    pub_sub = name(db, PubSub)
    Goblin.PubSub.unsubscribe(pub_sub)
  end

  @doc """
  Starts the Goblin database supervisor and all child processes.

  Creates the database directory if it doesn't exist and initializes all
  components including the Writer, Store, Compactor, WAL, and Manifest.

  ## Options

  - `:name` - Registered name for the supervisor (optional)
  - `:db_dir` - Directory path for database files (required)
  - `:key_limit` - Max keys in MemTable before flush (default: 50,000)
  - `:level_limit` - Size threshold for level 0 compaction in bytes (default: 128 MB)
  - `:bf_fpp` - Bloom filter false positive probability (default: 0.05)

  ## Returns

  - `{:ok, pid}` - On successful start
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        db_dir: "/var/lib/myapp/db",
        key_limit: 100_000,
        level_limit: 256 * 1024 * 1024,
        bf_fpp: 0.05
      )
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    db_dir = opts[:db_dir] || raise "no db_dir provided."
    name = opts[:name] || __MODULE__
    File.exists?(db_dir) || File.mkdir_p!(db_dir)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Stops the Goblin database supervisor.

  ## Parameters

  - `db` - Database supervisor pid or registered name
  - `reason` - Supervisor reason, defaults to `:normal`
  - `timeout` - Defaults to `:infinity`

  ## Returns

  - `:ok`
  """
  @spec stop(db_server(), term(), non_neg_integer | :infinity) :: :ok
  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(db, reason, timeout)
  end

  @impl true
  def init(args) do
    name = args[:name] || __MODULE__
    registry = name(name, ProcessRegistry)
    proc_sup = name(name, ProcessSupervisor)
    pub_sub = name(name, PubSub)
    writer = name(name, Writer)
    store = name(name, Store)
    compactor = name(name, Compactor)
    reader = name(name, Reader)
    manifest = name(name, Manifest)
    wal = name(name, WAL)
    task_sup = name(name, TaskSupervisor)

    names = %{
      writer: {writer, via(registry, writer)},
      store: {store, via(registry, store)},
      compactor: {compactor, via(registry, compactor)},
      reader: {reader, via(registry, reader)},
      manifest: {manifest, via(registry, manifest)},
      wal: {wal, via(registry, wal)},
      task_sup: {task_sup, via(registry, task_sup)}
    }

    children =
      [
        {Goblin.PubSub, name: pub_sub},
        {Goblin.ProcessRegistry, name: registry},
        {Goblin.ProcessSupervisor,
         Keyword.merge(
           args,
           name: via(registry, proc_sup),
           names: names,
           registry: registry,
           pub_sub: pub_sub
         )}
      ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp name(pid, suffix) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, []} ->
        name(__MODULE__, suffix)

      {:registered_name, registered_name} ->
        name(registered_name, suffix)
    end
  end

  defp name(nil, suffix), do: name(__MODULE__, suffix)
  defp name(name, suffix), do: Module.concat(name, suffix)
end
