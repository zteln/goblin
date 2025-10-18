defmodule SeaGoat do
  use Supervisor

  @type db_key_limit :: non_neg_integer()
  @type db_level_limit :: non_neg_integer()
  @type db_key :: term()
  @type db_value :: term() | nil
  @type db_level :: non_neg_integer()
  @type db_sequence :: non_neg_integer()
  @type db_file :: String.t()
  @type db_server :: GenServer.server()

  @default_key_limit 50_000
  @default_level_limit 128 * 1024 * 1024

  @spec put(db_server(), db_key(), db_value()) :: :ok
  def put(db, key, value) do
    writer = name(db, :writer)
    SeaGoat.Writer.put(writer, key, value)
  end

  # def put_multi do
  #
  # end

  @spec remove(db_server(), db_key()) :: :ok
  def remove(db, key) do
    writer = name(db, :writer)
    SeaGoat.Writer.remove(writer, key)
  end

  # def remove_multi do
  #
  # end

  @spec get(db_server(), db_key()) :: db_value() | nil
  def get(db, key, default \\ nil) do
    writer = name(db, :writer)
    store = name(db, :store)

    case SeaGoat.Reader.get(key, writer, store) do
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
    opts[:db_dir] || raise "no db_dir provided."
    Supervisor.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  @impl true
  def init(args) do
    db_dir = args[:db_dir]
    key_limit = args[:key_limit] || @default_key_limit
    level_limit = args[:level_limit] || @default_level_limit
    rw_locks_name = name(args[:name], :rw_locks)
    manifest_name = name(args[:name], :manifest)
    wal_name = name(args[:name], :wal)
    writer_name = name(args[:name], :writer)
    compactor_name = name(args[:name], :compactor)
    store_name = name(args[:name], :store)

    children = [
      {SeaGoat.RWLocks, Keyword.merge(args, name: rw_locks_name)},
      {SeaGoat.Manifest, Keyword.merge(args, name: manifest_name, db_dir: db_dir)},
      {SeaGoat.WAL, Keyword.merge(args, name: wal_name, db_dir: db_dir)},
      {SeaGoat.Compactor,
       Keyword.merge(args,
         name: compactor_name,
         key_limit: key_limit,
         level_limit: level_limit,
         manifest: manifest_name,
         store: store_name,
         rw_locks: rw_locks_name,
         level_limit: level_limit
       )},
      {SeaGoat.Store,
       Keyword.merge(args,
         name: store_name,
         dir: db_dir,
         writer: writer_name,
         manifest: manifest_name,
         rw_locks: rw_locks_name,
         compactor: compactor_name
       )},
      {SeaGoat.Writer,
       Keyword.merge(args,
         name: writer_name,
         key_limit: key_limit,
         wal: wal_name,
         store: store_name,
         manifest: manifest_name,
         limit: key_limit
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
    if name, do: :"#{name}_#{suffix}", else: :"sea_goat_#{suffix}"
  end
end
