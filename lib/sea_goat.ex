defmodule SeaGoat do
  use Supervisor

  defdelegate put(db \\ __MODULE__, key, value), to: SeaGoat.Writer
  defdelegate remove(db \\ __MODULE__, key), to: SeaGoat.Writer
  # defdelegate get(db \\ __MODULE__, key), to: SeaGoat.Server

  def start_link(opts) do
    opts[:dir] || raise "no dir provided."
    name = name(opts[:name], :sup)
    Supervisor.start_link(__MODULE__, opts, name: name)
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
       tier_limit: opts[:tier_limit]},
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

  defp name(name, suffix) do
    if name, do: :"#{name}_#{suffix}", else: :"sea_goat_#{suffix}"
  end
end
