defmodule SeaGoat do
  use Supervisor

  defdelegate put(db \\ __MODULE__, key, value), to: SeaGoat.Server
  defdelegate remove(db \\ __MODULE__, key), to: SeaGoat.Server
  defdelegate get(db \\ __MODULE__, key), to: SeaGoat.Server

  def start_link(opts) do
    opts[:dir] || raise "no dir provided."
    name = name(opts[:name], :sup)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    lock_manager_name = name(opts[:name], :lock_manager)
    wal_name = name(opts[:name], :wal)
    # flusher_name = name(opts[:name], :flusher)
    # merger_name = name(opts[:name], :merger)

    children = [
      {SeaGoat.LockManager, name: lock_manager_name},
      {SeaGoat.WAL, name: wal_name, dir: opts[:dir], sync_interval: opts[:sync_interval]},
      # {SeaGoat.Flusher, dir: opts[:dir], lock_manager: lock_manager_name, name: flusher_name},
      # {SeaGoat.Merger, dir: opts[:dir], lock_manager: lock_manager_name, name: merger_name},
      {
        SeaGoat.Server,
        # merger: merger_name,
        # flusher: flusher_name,
        name: opts[:name] || __MODULE__,
        dir: opts[:dir],
        wal: wal_name,
        lock_manager: lock_manager_name,
        limit: opts[:limit],
        level_limit: opts[:level_limit]
      }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp name(name, suffix) do
    if name, do: :"#{name}_#{suffix}", else: :"sea_goat_#{suffix}"
  end
end
