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
      writer: {local_writer_name, writer_name},
      store: {local_store_name, store_name},
      manifest: {local_manifest_name, manifest_name},
      wal: {local_wal_name, wal_name},
      compactor: {_compactor, compactor_name},
      reader: {local_reader_name, reader_name},
      task_sup: {_, task_sup_name}
    } = args[:names]

    children = [
      {Task.Supervisor, name: task_sup_name},
      {Goblin.Manifest,
       Keyword.merge(
         args,
         name: manifest_name,
         local_name: local_manifest_name,
         db_dir: db_dir
       )},
      {Goblin.WAL,
       Keyword.merge(
         args,
         name: wal_name,
         local_name: local_wal_name,
         db_dir: db_dir
       )},
      {Goblin.Compactor,
       Keyword.merge(args,
         name: compactor_name,
         store: store_name,
         reader: reader_name,
         manifest: manifest_name,
         task_sup: task_sup_name,
         key_limit: key_limit,
         level_limit: level_limit
       )},
      {Goblin.Store,
       Keyword.merge(args,
         name: store_name,
         local_name: local_store_name,
         compactor: compactor_name,
         manifest: manifest_name,
         dir: db_dir
       )},
      {Goblin.Reader,
       Keyword.merge(args,
         name: reader_name,
         local_name: local_reader_name
       )},
      {Goblin.Writer,
       Keyword.merge(args,
         name: writer_name,
         local_name: local_writer_name,
         reader: reader_name,
         store: {local_store_name, store_name},
         manifest: manifest_name,
         wal: wal_name,
         pub_sub: pub_sub,
         task_sup: task_sup_name,
         key_limit: key_limit
       )}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
