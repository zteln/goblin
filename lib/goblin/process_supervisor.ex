defmodule Goblin.ProcessSupervisor do
  @moduledoc false
  use Supervisor

  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10
  @default_bf_fpp 0.01

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(args) do
    db_dir = args[:db_dir]
    flush_level_file_limit = args[:flush_level_file_limit] || @default_flush_level_file_limit
    mem_limit = args[:mem_limit] || @default_mem_limit
    level_base_size = args[:level_base_size] || @default_level_base_size
    level_size_multiplier = args[:level_size_multiplier] || @default_level_size_multiplier
    bf_fpp = args[:bf_fpp] || @default_bf_fpp
    max_sst_size = div(level_base_size, 10)
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
         db_dir: db_dir,
         task_sup: task_sup_name
       )},
      {Goblin.Reader,
       Keyword.merge(args,
         name: reader_name,
         local_name: local_reader_name
       )},
      {Goblin.WAL,
       Keyword.merge(
         args,
         name: wal_name,
         local_name: local_wal_name,
         db_dir: db_dir,
         manifest: manifest_name,
         reader: reader_name,
         task_sup: task_sup_name
       )},
      {Goblin.Compactor,
       Keyword.merge(args,
         name: compactor_name,
         store: store_name,
         reader: reader_name,
         manifest: manifest_name,
         task_sup: task_sup_name,
         bf_fpp: bf_fpp,
         max_sst_size: max_sst_size,
         flush_level_file_limit: flush_level_file_limit,
         level_base_size: level_base_size,
         level_size_multiplier: level_size_multiplier
       )},
      {Goblin.Store,
       Keyword.merge(args,
         name: store_name,
         local_name: local_store_name,
         compactor: compactor_name,
         manifest: manifest_name,
         dir: db_dir
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
         bf_fpp: bf_fpp,
         max_sst_size: max_sst_size,
         mem_limit: mem_limit
       )}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
