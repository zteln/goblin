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
  @type db_file :: String.t()
  @type db_server :: GenServer.server()

  @default_key_limit 50_000
  @default_level_limit 128 * 1024 * 1024

  @spec transaction(db_server(), (Goblin.Transaction.t() -> Goblin.Writer.transaction_return())) :: term() | :ok | {:error, term()}
  def transaction(db, f) do
    writer = name(db, :writer)
    Goblin.Writer.transaction(writer, f)
  end

  @spec put(db_server(), db_key(), db_value()) :: :ok
  def put(db, key, value) do
    writer = name(db, :writer)
    Goblin.Writer.put(writer, key, value)
  end

  def put_multi(db, pairs) do
    writer = name(db, :writer)
    Goblin.Writer.put_multi(writer, pairs)
  end

  @spec remove(db_server(), db_key()) :: :ok
  def remove(db, key) do
    writer = name(db, :writer)
    Goblin.Writer.remove(writer, key)
  end

  def remove_multi(db, keys) do
    writer = name(db, :writer)
    Goblin.Writer.remove_multi(writer, keys)
  end

  @spec get(db_server(), db_key()) :: db_value() | nil
  def get(db, key, default \\ nil) do
    writer = name(db, :writer)
    store = name(db, :store)

    case Goblin.Reader.get(key, writer, store) do
      :not_found -> default
      {_seq, value} -> value
    end
  end

  # def get_multi do
  #
  # end

  # def select do
  #
  # end

  # def is_compacting do
  #
  # end
  #
  # def is_flushing do
  #
  # end

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
