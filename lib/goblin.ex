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
  use Supervisor
  import Goblin.Registry, only: [via: 2]

  @typedoc false
  @type level_key :: -1 | non_neg_integer()
  @typedoc false
  @type seq_no :: non_neg_integer()
  @typedoc false
  @type pair :: {Goblin.db_key(), Goblin.db_value()}
  @typedoc false
  @type triple :: {Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()}
  @typedoc false
  @type server :: pid() | module() | {:via, Registry, {module(), module()}}
  @typedoc false
  @type write_term :: {:put, seq_no(), db_key(), db_value()} | {:remove, seq_no(), db_key()}

  @type db_tag :: term()
  @type db_key :: term()
  @type db_value :: term()

  @doc """
  Executes a function within a write transaction.

  Transactions are executed serially and are ACID-compliant.
  The provided function receives a transaction struct and must return
  `{:commit, tx, reply}` to commit, or `:abort` to abort.
  Returning anything else raises.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `f` - A function that takes a `Goblin.Tx.t()` and returns a transaction result

  ## Returns

  - `reply` - The value from `{:commit, tx, reply}` when committed
  - `{:error, :aborted}` - When the transaction is aborted

  ## Examples

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)
        tx = Goblin.Tx.put(tx, :counter, counter + 1)
        {:commit, tx, :ok}
      end)
      # => :ok

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
    broker = child_name(namespace, Broker)
    Goblin.Broker.write_transaction(broker, via(registry, broker), f)
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
  @spec put(Supervisor.supervisor(), db_key(), db_value()) :: :ok
  def put(db, key, value, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.put(tx, key, value, Keyword.take(opts, [:tag])), :ok}
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
  @spec put_multi(Supervisor.supervisor(), [{db_key(), db_value()}], keyword()) :: :ok
  def put_multi(db, pairs, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.put_multi(tx, pairs, Keyword.take(opts, [:tag])), :ok}
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
  @spec remove(Supervisor.supervisor(), db_key(), keyword()) :: :ok
  def remove(db, key, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.remove(tx, key, Keyword.take(opts, [:tag])), :ok}
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
  @spec remove_multi(Supervisor.supervisor(), [db_key()], keyword()) :: :ok
  def remove_multi(db, keys, opts \\ []) do
    transaction(db, fn tx ->
      {:commit, Goblin.Tx.remove_multi(tx, keys, Keyword.take(opts, [:tag])), :ok}
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
  @spec read(Supervisor.supervisor(), (Goblin.Tx.t() -> any())) :: any()
  def read(db, f) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    broker = child_name(namespace, Broker)
    mem_tables = child_name(namespace, MemTables)
    Goblin.Broker.read_transaction(broker, via(registry, broker), mem_tables, f)
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
  @spec get(Supervisor.supervisor(), db_key(), keyword()) :: db_value() | nil
  def get(db, key, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.get(tx, key, Keyword.take(opts, [:tag, :default]))
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
  @spec get_multi(Supervisor.supervisor(), [db_key()], keyword()) :: [{db_key(), db_value()}]
  def get_multi(db, keys, opts \\ []) do
    read(db, fn tx ->
      Goblin.Tx.get_multi(tx, keys, Keyword.take(opts, [:tag]))
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

      Goblin.select(db) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.select(db, min: :bob) |> Enum.to_list()
      # => [{:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.select(db, min: :alice, max: :bob) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}]
  """
  @spec select(Supervisor.supervisor(), keyword()) :: Enumerable.t({db_key(), db_value()})
  def select(db, opts \\ []) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    broker = child_name(namespace, Broker)
    mem_tables = child_name(namespace, MemTables)

    Goblin.Broker.select(
      broker,
      via(registry, broker),
      mem_tables,
      Keyword.take(opts, [:min, :max, :tag])
    )
  end

  @doc """
  Exports a snapshot of the database as a `.tar.gz` archive.

  The archive can be unpacked and used as the `data_dir` for a new
  database instance, acting as a backup.

  The export runs inside a read transaction, preventing file deletion
  while the snapshot is being created.

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
  Returns whether background compaction is currently running.
  """
  @spec compacting?(Supervisor.supervisor()) :: boolean()
  def compacting?(db) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    disk_tables = child_name(namespace, DiskTables)
    Goblin.DiskTables.compacting?(via(registry, disk_tables))
  end

  @doc """
  Returns whether a memory-to-disk flush is currently running.
  """
  @spec flushing?(Supervisor.supervisor()) :: boolean()
  def flushing?(db) do
    namespace = namespace(db)
    registry = child_name(namespace, Registry)
    mem_tables = child_name(namespace, MemTables)
    Goblin.MemTables.flushing?(via(registry, mem_tables))
  end

  @doc """
  Subscribes the calling process to write notifications.

  After subscribing, the process receives `{:put, key, value}` or
  `{:remove, key}` messages for each committed write. Batch writes
  produce one message per key.

  ## Parameters

  - `db` - The database server (PID or registered name)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.subscribe(db)
      # => :ok

      Goblin.put(db, :alice, "Alice")

      receive do
        {:put, :alice, value} -> value
      end
      # => "Alice"
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
  Unsubscribes the calling process from write notifications.
  """
  @spec unsubscribe(Supervisor.supervisor()) :: :ok | {:error, term()}
  def unsubscribe(db) do
    namespace = namespace(db)
    pub_sub = child_name(namespace, PubSub)
    Goblin.PubSub.unsubscribe(pub_sub)
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
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    data_dir = opts[:data_dir] || raise "no data_dir provided."
    name = opts[:name] || __MODULE__
    File.exists?(data_dir) || File.mkdir_p!(data_dir)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Stops the database.
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
