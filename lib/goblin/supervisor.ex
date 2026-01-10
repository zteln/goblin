defmodule Goblin.Supervisor do
  @moduledoc false
  use Supervisor

  import Goblin.Registry, only: [via: 2]
  # import Goblin, only: [child_name: 2]

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
    namespace = args[:namespace]
    registry = args[:registry]
    db_dir = args[:db_dir]
    flush_level_file_limit = args[:flush_level_file_limit] || @default_flush_level_file_limit
    mem_limit = args[:mem_limit] || @default_mem_limit
    level_base_size = args[:level_base_size] || @default_level_base_size
    level_size_multiplier = args[:level_size_multiplier] || @default_level_size_multiplier
    bf_fpp = args[:bf_fpp] || @default_bf_fpp
    max_sst_size = args[:max_sst_size] || div(level_base_size, level_size_multiplier)
    pub_sub = args[:pub_sub]

    manifest_name = Goblin.child_name(namespace, Manifest)
    cleaner_name = Goblin.child_name(namespace, Cleaner)
    wal_name = Goblin.child_name(namespace, WAL)
    compactor_name = Goblin.child_name(namespace, Compactor)
    broker_name = Goblin.child_name(namespace, Broker)
    disk_tables_name = Goblin.child_name(namespace, DiskTables)
    mem_table_name = Goblin.child_name(namespace, MemTable)

    children = [
      {Goblin.Manifest,
       Keyword.merge(
         args,
         name: via(registry, manifest_name),
         local_name: manifest_name,
         db_dir: db_dir
       )},
      {Goblin.Cleaner,
       Keyword.merge(
         args,
         name: via(registry, cleaner_name),
         local_name: cleaner_name,
         disk_tables_server: via(registry, disk_tables_name)
       )},
      {Goblin.WAL,
       Keyword.merge(
         args,
         name: via(registry, wal_name),
         local_name: wal_name,
         db_dir: db_dir,
         manifest_server: via(registry, manifest_name),
         cleaner_server: via(registry, cleaner_name)
       )},
      {Goblin.Compactor,
       Keyword.merge(args,
         name: via(registry, compactor_name),
         manifest_server: via(registry, manifest_name),
         disk_tables_server: via(registry, disk_tables_name),
         cleaner_server: via(registry, cleaner_name),
         flush_level_file_limit: flush_level_file_limit,
         level_base_size: level_base_size,
         level_size_multiplier: level_size_multiplier
       )},
      {Goblin.DiskTables,
       Keyword.merge(args,
         name: via(registry, disk_tables_name),
         local_name: disk_tables_name,
         db_dir: db_dir,
         manifest_server: via(registry, manifest_name),
         compactor_server: via(registry, compactor_name),
         bf_fpp: bf_fpp,
         max_sst_size: max_sst_size
       )},
      {Goblin.MemTable,
       Keyword.merge(args,
         name: via(registry, mem_table_name),
         local_name: mem_table_name,
         wal_server: via(registry, wal_name),
         manifest_server: via(registry, manifest_name),
         disk_tables_server: via(registry, disk_tables_name),
         mem_limit: mem_limit
       )},
      {Goblin.Broker,
       Keyword.merge(args,
         name: via(registry, broker_name),
         mem_table: via(registry, mem_table_name),
         mem_table_store: mem_table_name,
         disk_tables_store: disk_tables_name,
         cleaner_table: cleaner_name,
         cleaner_server: via(registry, cleaner_name),
         pub_sub: pub_sub
       )}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
