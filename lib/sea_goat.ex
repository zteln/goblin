defmodule SeaGoat do
  use Supervisor

  def put(db, key, value) do
    writer = name(db, :writer)
    SeaGoat.Writer.put(writer, key, value)
  end

  def remove(db, key) do
    writer = name(db, :writer)
    SeaGoat.Writer.remove(writer, key)
  end

  def get(db, key) do
    writer = name(db, :writer)
    store = name(db, :store)
    SeaGoat.Reader.get(writer, store, key)
  end

  def start_link(opts) do
    opts[:dir] || raise "no dir provided."
    Supervisor.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  @impl true
  def init(opts) do
    rw_locks_name = name(opts[:name], :rw_locks)
    wal_name = name(opts[:name], :wal)
    writer_name = name(opts[:name], :writer)
    compactor_name = name(opts[:name], :compactor)
    store_name = name(opts[:name], :store)

    children = [
      {SeaGoat.RWLocks, name: rw_locks_name},
      {SeaGoat.WAL, name: wal_name, sync_interval: opts[:sync_interval]},
      {SeaGoat.Writer, name: writer_name, wal: wal_name, store: store_name, limit: opts[:limit]},
      {SeaGoat.Compactor,
       name: compactor_name,
       wal: wal_name,
       store: store_name,
       rw_locks: rw_locks_name,
       level_limit: opts[:level_limit]},
      {
        SeaGoat.Store,
        name: store_name,
        dir: opts[:dir],
        writer: writer_name,
        wal: wal_name,
        rw_locks: rw_locks_name,
        compactor: compactor_name
      }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp name(name, suffix) when is_pid(name) do
    case Process.info(name, :registered_name) do
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
