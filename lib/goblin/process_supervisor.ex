defmodule Goblin.ProcessSupervisor do
  @moduledoc false
  use Supervisor
  import Goblin.ProcessRegistry, only: [via: 1, via: 2]

  @default_key_limit 50_000
  @default_level_limit 128 * 1024 * 1024

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    registry = opts[:registry]
    Supervisor.start_link(__MODULE__, opts, name: via(registry))
  end

  @impl true
  def init(args) do
    db_dir = args[:db_dir]
    key_limit = args[:key_limit] || @default_key_limit
    level_limit = args[:level_limit] || @default_level_limit

    registry = args[:registry]
    pub_sub = args[:pub_sub]
    task_sup = via(registry, Goblin.TaskSupervisor)

    children = [
      {Task.Supervisor, name: task_sup},
      {Goblin.RWLocks, Keyword.merge(args, registry: registry)},
      {Goblin.Manifest, Keyword.merge(args, registry: registry, db_dir: db_dir)},
      {Goblin.WAL, Keyword.merge(args, registry: registry, db_dir: db_dir)},
      {Goblin.Compactor,
       Keyword.merge(args,
         registry: registry,
         task_sup: task_sup,
         key_limit: key_limit,
         level_limit: level_limit
       )},
      {Goblin.Store,
       Keyword.merge(args,
         registry: registry,
         dir: db_dir
       )},
      {Goblin.Writer,
       Keyword.merge(args,
         registry: registry,
         pub_sub: pub_sub,
         task_sup: task_sup,
         key_limit: key_limit
       )}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
