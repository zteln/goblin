defmodule Goblin.ProcessSupervisor do
  @moduledoc false
  use Supervisor

  @default_key_limit 50_000
  @default_level_limit 128 * 1024 * 1024

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(args) do
    db_dir = args[:db_dir]
    key_limit = args[:key_limit] || @default_key_limit
    level_limit = args[:level_limit] || @default_level_limit
    pub_sub = args[:pub_sub]

    %{
      writer: {writer, registered_writer},
      store: {store, registered_store},
      manifest: {manifest, registered_manifest},
      wal: {wal, registered_wal},
      compactor: {_compactor, registered_compactor},
      task_sup: {_, registered_task_sup}
    } = args[:names]

    children = [
      {Task.Supervisor, name: registered_task_sup},
      {Goblin.Manifest,
       Keyword.merge(
         args,
         name: registered_manifest,
         local_name: manifest,
         db_dir: db_dir
       )},
      {Goblin.WAL,
       Keyword.merge(
         args,
         name: registered_wal,
         local_name: wal,
         db_dir: db_dir
       )},
      {Goblin.Compactor,
       Keyword.merge(args,
         name: registered_compactor,
         store: registered_store,
         manifest: registered_manifest,
         task_sup: registered_task_sup,
         key_limit: key_limit,
         level_limit: level_limit
       )},
      {Goblin.Store,
       Keyword.merge(args,
         name: registered_store,
         local_name: store,
         compactor: registered_compactor,
         manifest: registered_manifest,
         dir: db_dir
       )},
      {Goblin.Writer,
       Keyword.merge(args,
         name: registered_writer,
         local_name: writer,
         store: registered_store,
         store_name: store,
         manifest: registered_manifest,
         wal: registered_wal,
         pub_sub: pub_sub,
         task_sup: registered_task_sup,
         key_limit: key_limit
       )}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
