defmodule Goblin.Supervisor do
  @moduledoc false
  use Supervisor

  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(args) do
    namespace = args[:namespace]
    registry = args[:registry]
    data_dir = args[:data_dir]
    flush_level_file_limit = args[:flush_level_file_limit] || @default_flush_level_file_limit
    mem_limit = args[:mem_limit] || @default_mem_limit
    level_base_size = args[:level_base_size] || @default_level_base_size
    level_size_multiplier = args[:level_size_multiplier] || @default_level_size_multiplier
    max_sst_size = args[:max_sst_size] || div(level_base_size, level_size_multiplier)
    pub_sub = args[:pub_sub]

    manifest = Goblin.child_name(namespace, Manifest)
    broker = Goblin.child_name(namespace, Broker)
    disk_tables = Goblin.child_name(namespace, DiskTables)
    mem_tables = Goblin.child_name(namespace, MemTables)

    children = [
      {Goblin.Manifest,
       Keyword.merge(
         args,
         name: manifest,
         data_dir: data_dir,
         registry: registry
       )},
      {Goblin.Broker,
       Keyword.merge(args,
         name: broker,
         mem_tables: mem_tables,
         pub_sub: pub_sub,
         registry: registry
       )},
      {Goblin.DiskTables,
       Keyword.merge(args,
         name: disk_tables,
         data_dir: data_dir,
         manifest: manifest,
         broker: broker,
         registry: registry,
         bf_fpp: args[:bf_fpp],
         max_sst_size: max_sst_size,
         flush_level_file_limit: flush_level_file_limit,
         level_base_size: level_base_size,
         level_size_multiplier: level_size_multiplier
       )},
      {Goblin.MemTables,
       Keyword.merge(args,
         name: mem_tables,
         data_dir: data_dir,
         mem_limit: mem_limit,
         manifest: manifest,
         broker: broker,
         disk_tables: disk_tables,
         registry: registry
       )}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
