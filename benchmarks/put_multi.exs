Code.require_file("support.exs", __DIR__)
alias Bench.Support

goblin_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put_multi/goblin"
cubdb_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put_multi/cubdb"

Support.run(
  "put_multi",
  %{
    "Goblin.put_multi/2" => fn {goblin, _cubdb, pairs} ->
      Goblin.put_multi(goblin, pairs)
    end
  },
  %{
    "CubDB.put_multi/2" => fn {_goblin, cubdb, pairs} ->
      CubDB.put_multi(cubdb, pairs)
    end
  },
  inputs: %{
    "10" => 10,
    "100" => 100,
    "1_000" => 1_000,
    "10_000" => 10_000,
    "100_000" => 100_000
  },
  before_scenario: fn size ->
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
    goblin = Support.start_goblin(goblin_dir)
    cubdb = Support.start_cubdb(cubdb_dir, auto_compact: false, auto_file_sync: true)
    {goblin, cubdb, size}
  end,
  before_each: fn {goblin, cubdb, size} ->
    pairs =
      for _ <- 1..size do
        {:rand.uniform(100_000), :crypto.strong_rand_bytes(Support.value_size())}
      end

    {goblin, cubdb, pairs}
  end,
  after_scenario: fn {goblin, cubdb, _size} ->
    Support.stop_goblin(goblin)
    Support.stop_cubdb(cubdb)
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
  end
)
