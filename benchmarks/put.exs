Code.require_file("support.exs", __DIR__)
alias Bench.Support

goblin_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put/goblin"
cubdb_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put/cubdb"

Support.run(
  "put",
  %{
    "Goblin.put/3" => fn {goblin, _cubdb, key, value} ->
      Goblin.put(goblin, key, value)
    end
  },
  %{
    "CubDB.put/3" => fn {_goblin, cubdb, key, value} ->
      CubDB.put(cubdb, key, value)
    end
  },
  inputs: Support.value_size_inputs(),
  before_scenario: fn size ->
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
    goblin = Support.start_goblin(goblin_dir)
    cubdb = Support.start_cubdb(cubdb_dir, auto_compact: false, auto_file_sync: true)
    {goblin, cubdb, size}
  end,
  before_each: fn {goblin, cubdb, size} ->
    key = :rand.uniform(100_000)
    {goblin, cubdb, key, :crypto.strong_rand_bytes(size)}
  end,
  after_scenario: fn {goblin, cubdb, _size} ->
    Support.stop_goblin(goblin)
    Support.stop_cubdb(cubdb)
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
  end
)
