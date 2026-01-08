Mix.install([{:goblin, path: File.cwd!()}], force: true)

db_dir = Path.join([File.cwd!(), "tmp", "goblin_put_profile"])
File.rm_rf!(db_dir)

{:ok, db} = Goblin.start_link(db_dir: db_dir)

[
  {_, proc_sup, _, _},
  {_registry, _, _, _},
  _
] = Supervisor.which_children(db)

[
  {Goblin.Broker, broker, _, _},
  {Goblin.MemTable, mem_table, _, _},
  {Goblin.DiskTables, _disk_tables, _, _},
  {Goblin.Compactor, _compactor, _, _},
  {Goblin.WAL, wal, _, _},
  {Goblin.Cleaner, _cleaner, _, _},
  {Goblin.Manifest, _manifest, _, _}
] = Supervisor.which_children(proc_sup)

spec = [
  broker,
  mem_table,
  wal
]

:tprof.start(%{type: :call_time})
:tprof.set_pattern(:_, :_, :_)
:tprof.enable_trace(spec, :all_children)

Goblin.put(db, :rand.uniform(10_000), :crypto.strong_rand_bytes(10 * 1024 * 1024))

Process.sleep(2000)

sample = :tprof.collect()

IO.puts("""
Legend:
broker:     #{inspect(broker)}
mem_table:  #{inspect(mem_table)}
wal:        #{inspect(wal)}
""")

:tprof.format(:tprof.inspect(sample))
