Code.require_file("support.exs", __DIR__)
alias Bench.Support

Support.require_fixtures!()

range_size = 100

Support.run(
  "scan",
  %{
    "Goblin.scan/2" => fn {goblin, _cubdb, min, max} ->
      Goblin.scan(goblin, min: min, max: max) |> Enum.to_list()
    end
  },
  %{
    "CubDB.select/2" => fn {_goblin, cubdb, min, max} ->
      CubDB.select(cubdb, min_key: min, max_key: max) |> Enum.to_list()
    end
  },
  inputs: Support.dataset_inputs(),
  before_scenario: fn repo ->
    goblin = Support.start_goblin(Support.goblin_fixture(repo))
    cubdb = Support.start_cubdb(Support.cubdb_fixture(repo))
    _ = Goblin.get(goblin, 1)
    {goblin, cubdb, Support.num_keys(repo)}
  end,
  before_each: fn {goblin, cubdb, num_keys} ->
    min = :rand.uniform(max(num_keys - range_size, 1))
    max = min + range_size
    {goblin, cubdb, min, max}
  end,
  after_scenario: fn {goblin, cubdb, _num_keys} ->
    Support.stop_goblin(goblin)
    Support.stop_cubdb(cubdb)
  end
)
