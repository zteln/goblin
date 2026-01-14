defmodule Goblin do
  @moduledoc """
  An embedded LSM-Tree database for Elixir.

  Goblin provides a persistent key-value store with ACID transactions,
  automatic compaction, and crash recovery. It uses a Log-Structured
  Merge-Tree architecture optimized for write-heavy workloads.

  ## Starting a database

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        db_dir: "/path/to/db"
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

      Goblin.read(db, fn tx ->
        key1 = Tx.get(tx, :start, 0)
        key2 = Tx.get(tx, key1)
        {key1, key2}
      end)

  ## Configuration

  - `name` - Registered name for the database supervisor (optional)
  - `db_dir` - Directory path for storing database files (required)
  - `flush_level_file_limit` - How many files in flush level before compaction is triggered (default: 4)
  - `mem_limit` - How many bytes are stored in memory before flushing (default: 64 * 1024 * 1024)
  - `level_base_size` - How many bytes are stored in level 1 before compaction is triggered (default: 256 * 1024 * 1024)
  - `level_size_multiplier` - Which factor each level size is multiplied with (default: 10)
  - `max_sst_size` - How large, in bytes, the SST portion of a disk table is allowed to be (default: level_base_size / level_size_multiplier)
  - `bf_fpp` - Bloom filter false positive probability (default: 0.01)

  ## Architecture

  Goblin consists of several supervised components:

  - **Broker** - Transactions management
  - **MemTable** - Memory buffer server
  - **DiskTables** - Disk table management
  - **Compactor** - Compaction server
  - **Cleaner** - Safe file removal server
  - **WAL** - Write-ahead log server
  - **Manifest** - Tracks database state and file metadata
  """
  use Supervisor
  import Goblin.Registry, only: [via: 2]

  @typedoc false
  @type level_key :: non_neg_integer()
  @typedoc false
  @type seq_no :: non_neg_integer()
  @typedoc false
  @type triple :: {Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()}
  @typedoc false
  @type server :: module() | {:via, Registry, {module(), module()}}
  @typedoc false
  @type write_term :: {:put, seq_no(), db_key(), db_value()} | {:remove, seq_no(), db_key()}

  @type db_key :: term()
  @type db_value :: term()

  @doc """
  Executes a function within a database transaction that allow writes.

  Transactions are executed serially and are ACID-compliant.
  The provided function, `f`, receives a transaction struct and must return either `{:commit, tx, reply}` in order to commit the transaction, or `:abort` to abort the transaction. 
  Returning anything else results in a raise.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `f` -A function that takes a `Goblin.Tx.t()` struct and returns a transaction result

  ## Returns

  - `reply` - The value returned from the transaction when successfully committed
  - `{:error, :aborted}` - When the transaction is aborted
  - `{:error, :already_in_tx}` - When attempting to create nested transactions
  - `{:error, :not_writer}` - When attempting to commit by somebody other than the current writer

  ## Examples

      # Update a value
      Goblin.transaction(db, fn tx -> 
        count = Goblin.Tx.get(tx, :counter, 0)
        tx = Goblin.Tx.put(tx, :counter, counter + 1)
        {:commit, tx, :ok}
      end)
      # => :ok

      # Abort a transaction
      Goblin.transaction(db, fn _tx -> 
        :abort
      end)
      # => {:error, :aborted}
  """
  @spec transaction(Supervisor.supervisor(), (Goblin.Tx.t() -> Goblin.Tx.return())) ::
          any() | {:error, :aborted | :already_in_tx | :not_writer}
  def transaction(db, f) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    broker = via(registry, child_name(namespace, Broker))
    Goblin.Broker.write_transaction(broker, f)
  end

  @doc """
  Writes a key-value pair to the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir to associate with `key`

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put(db, :user_123, %{name: "Alice", age: 30})
      # => :ok
  """
  @spec put(Supervisor.supervisor(), db_key(), db_value()) :: :ok
  def put(db, key, value, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.put(tx, key, value, opts), :ok}
    end)
  end

  @doc """
  Writes multiple key-value pairs in a single operation.

  This is more efficient than calling `put/3` multiple times as it batches the writes together.

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
  @spec put_multi(Supervisor.supervisor(), [{db_key(), db_value()}]) :: :ok
  def put_multi(db, pairs, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.put_multi(tx, pairs, opts), :ok}
    end)
  end

  @doc """
  Removes a key from the database.

  This operation writes a tombstone marker for the key.
  The actual data is eventually removed during compaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to remove

  ## Returns

  - `:ok`

  ## Examples
      Goblin.get(db, :user_123)
      # => %{name: "Alice", age: 30}

      Goblin.remove(db, :user_123)
      # => :ok

      Goblin.get(db, :user_123)
      # => nil
  """
  @spec remove(Supervisor.supervisor(), db_key()) :: :ok
  def remove(db, key, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.remove(tx, key, opts), :ok}
    end)
  end

  @doc """
  Removes multiple keys from the database in a single operation.

  This is more efficient than calling `remove/2` multiple times as it batches the removals together.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to remove

  ## Returns

  - `:ok`


  ## Examples

      Goblin.remove_multi(db, [:user_1, :user_2, :user_3])
      # => :ok
  """
  @spec remove_multi(Supervisor.supervisor(), [db_key()]) :: :ok
  def remove_multi(db, keys, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.remove_multi(tx, keys, opts), :ok}
    end)
  end

  @doc """
  Performs a read transaction.
  A snapshot is taken in order to provide consistent reads of the database.
  This allows concurrent readers and will not block each other.

  Attempting to write to the database during this transactions results in a raise.

  Any file deletion is prevented while there exists an ongoing read transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `f` - A function that takes a `Goblin.Tx.t()` struct

  ## Returns

  - The evaluation of `f`

  ## Examples

      # Get two values within snapshot
      Goblin.read(db, fn tx -> 
        key1 = Goblin.Tx.get(tx, :start, 0)
        key2 = Goblin.Tx.get(tx, key1)
        {key1, key2}
      end)
      # => {key1, key2}

      # Attempting to write will raise
      Goblin.read(db, fn tx -> 
        tx = Goblin.Tx.put(tx, :user_123, %{name: "Alice", age:30})
        {:commit, tx, :ok}
      end)
      # => RuntimeError
  """
  @spec read(Supervisor.supervisor(), (Goblin.Tx.t() -> any())) :: any()
  def read(db, f) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    cleaner = child_name(namespace, Cleaner)
    cleaner_server = via(registry, cleaner)
    mem_table = child_name(namespace, MemTable)
    disk_tables = child_name(namespace, DiskTables)
    Goblin.Broker.read_transaction(cleaner, cleaner_server, mem_table, disk_tables, f)
  end

  @doc """
  Retrieves the value associated with the provided key.
  Returns the default value of the key was not found.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to look up
  - `default` - Value to return if `key` is not found (default: `nil`)

  ## Returns

  - The value associated with the key, or `default` if not found

  ## Examples

      Goblin.get(db, :user_123)
      # => nil

      Goblin.put(db, :user_123, %{name: "Alice", age: 30})

      Goblin.get(db, :user_123)
      # => %{name: "Alice", age: 30}

      Goblin.get(db, :non_existant_key, :default_value)
      # => :default_value
  """
  @spec get(Supervisor.supervisor(), db_key(), db_value() | nil) :: db_value() | nil
  def get(db, key, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.get(tx, key, opts)
    end)
  end

  @doc """
  Retrieves values for multiple keys in a single operation.
  If a key in the provided list of keys is not found then it is excluded from the resulting list of key-value pairs.

  If a lot of keys are already flushed to disk, then this operation is more efficient than calling `get/3` multiple times.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to look up

  ## Returns

  - A list of `{key, value}` tuples for keys found, sorted by key

  ## Examples

      Goblin.put_multi(db, [
        {:user_1, %{name: "Alice"}},
        {:user_2, %{name: "Bob"}}
      ])

      Goblin.get_multi(db, [:user_1, :user_2])
      # => [user_1: %{name: "Alice"}, user_2: %{name: "Bob"}]

      Goblin.get_multi(db, [:user_1, :user_2, :non_existing_user])
      # => [user_1: %{name: "Alice"}, user_2: %{name: "Bob"}]
  """
  @spec get_multi(Supervisor.supervisor(), [db_key()]) :: [{db_key(), db_value()}]
  def get_multi(db, keys, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.get_multi(tx, keys, opts)
    end)
  end

  @doc """
  Retrieves all key-value pairs within a specified range in the form of a stream.

  Entries in the stream are sorted by key in ascending order.
  Both `min` and `max` are inclusive.
  If neither bound is specified, then all key-value pairs are retrieved.
  Specifying only `min` returns all key-value pairs from `min` and onwards.
  Specifying only `max` returns all key-value pairs until `max`.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `opts` - Keyword list of options:
    - `:min` - Minimum key (inclusive, optional)
    - `:max` - Maximum key (inclusive, optional)

  ## Returns

  - A stream over the key-value pairs in the provided range

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
  @spec select(Supervisor.supervisor(), keyword()) :: Enumerable.t({db_key(), db_value()})
  def select(db, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.select(tx, Keyword.take(opts, [:min, :max, :tag]))
    end)
  end

  @doc """
  Exports the database files to the provided dir, `export_dir`.
  `export` can be used to create back-ups of the database.
  A copy of the manifest is created and the currently tracked files along with the manifest copy is archived in a .tar.gz file in `export_dir`.

  > #### Note {: .warning}
  > 
  > The export runs in a `read/2`, thus preventing deletion of files.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `export_dir` - Path to the directory to place the exported .tar.gz

  ## Returns

  - `{:ok, export_path}` - Path of the exported .tar.gz file
  - `{:error, reason}` - If an error occurred

  ## Examples

      Goblin.export(db, "path/to/back_ups")
      # => {:ok, "path/to/back_ups/goblin_20260106T102414Z.tar.g"}
  """
  @spec export(Supervisor.supervisor(), Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def export(db, export_dir) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    manifest_server = via(registry, child_name(namespace, Manifest))

    read(db, fn _tx ->
      Goblin.Export.export(export_dir, manifest_server)
    end)
  end

  @doc """
  Checks whether the database is currently compacting or not.

  ## Parameters

  - `db` - The dabase server (PID or registered name)

  ## Returns

  - A boolean indicating whether the database is compacting or not
  """
  @spec compacting?(Supervisor.supervisor()) :: boolean()
  def compacting?(db) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    compactor = via(registry, child_name(namespace, Compactor))
    Goblin.Compactor.compacting?(compactor)
  end

  @doc """
  Checks whether the database is currently flushing or not.

  ## Parameters

  - `db` - The database server (PID or registered name)

  ## Returns

  - A boolean indicating whether the database is flushing or not
  """
  @spec flushing?(Supervisor.supervisor()) :: boolean()
  def flushing?(db) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    mem_table_server = via(registry, child_name(namespace, MemTable))
    Goblin.MemTable.flushing?(mem_table_server)
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
  @spec subscribe(Supervisor.supervisor()) :: :ok | {:error, term()}
  def subscribe(db) do
    namespace = namespace(db)
    pub_sub = child_name(namespace, PubSub)

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
  @spec unsubscribe(Supervisor.supervisor()) :: :ok | {:error, term()}
  def unsubscribe(db) do
    namespace = namespace(db)
    pub_sub = child_name(namespace, PubSub)
    Goblin.PubSub.unsubscribe(pub_sub)
  end

  @doc """
  Starts the Goblin database supervisor and all child processes.

  Creates the database directory if it doesn't exist and initializes all
  components including the Writer, Store, Compactor, WAL, and Manifest.

  ## Options

  - `:name` - Registered name for the supervisor (optional)
  - `:db_dir` - Directory path for database files (required)
  - `:flush_level_file_limit` - How many files in flush level before compaction is triggered (default: 4)
  - `:mem_limit` - How many bytes are stored in memory before flushing (default: 64 * 1024 * 1024)
  - `:level_base_size` - How many bytes are stored in level 1 before compaction is triggered (default: 256 * 1024 * 1024)
  - `:level_size_multiplier` - Which factor each level size is multiplied with (default: 10)
  - `:max_sst_size` - How large, in bytes, the SST portion of a disk table is allowed to be (default: level_base_size / level_size_multiplier)
  - `:bf_fpp` - Bloom filter false positive probability (default: 0.01)

  ## Returns

  - `{:ok, pid}` - On successful start
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        db_dir: "/var/lib/myapp/db",
        flush_level_file_limit: 4,
        mem_limit: 64 * 1024 * 1024,
        level_base_size: 256 * 1024 * 1024,
        level_size_multiplier: 10,
        bf_fpp: 0.01
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
  @spec stop(Supervisor.supervisor(), term(), non_neg_integer | :infinity) :: :ok
  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(db, reason, timeout)
  end

  @impl true
  def init(args) do
    namespace = args[:name] || __MODULE__
    registry = child_name(namespace, Registry)
    sup = child_name(namespace, Supervisor)
    pub_sub = child_name(namespace, PubSub)

    children =
      [
        {Goblin.PubSub, name: pub_sub},
        {Goblin.Registry, name: registry},
        {Goblin.Supervisor,
         Keyword.merge(
           args,
           name: sup,
           namespace: namespace,
           registry: registry,
           pub_sub: pub_sub
         )}
      ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def child_name(namespace, suffix), do: Module.concat(namespace, suffix)

  defp namespace(pid) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, []} -> __MODULE__
      {:registered_name, namespace} -> namespace
    end
  end

  defp namespace(namespace), do: namespace
end
