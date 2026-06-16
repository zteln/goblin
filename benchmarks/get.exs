Code.require_file("support.exs", __DIR__)
alias Bench.Support

Support.require_fixtures!()

# Fixtures populate keys 1..num_keys, so a key in num_keys+1..2*num_keys is
# guaranteed absent — that exercises the bloom-filter reject path (a "miss").
Support.run(
  "get",
  %{
    "Goblin.get/2 (hit)" => fn {goblin, _cubdb, hit, _miss} ->
      Goblin.get(goblin, hit)
    end,
    "Goblin.get/2 (miss)" => fn {goblin, _cubdb, _hit, miss} ->
      Goblin.get(goblin, miss)
    end
  },
  %{
    "CubDB.get/2 (hit)" => fn {_goblin, cubdb, hit, _miss} ->
      CubDB.get(cubdb, hit)
    end,
    "CubDB.get/2 (miss)" => fn {_goblin, cubdb, _hit, miss} ->
      CubDB.get(cubdb, miss)
    end
  },
  inputs: Support.dataset_inputs(),
  before_scenario: fn repo ->
    goblin = Support.start_goblin(Support.goblin_fixture(repo))
    cubdb = Support.start_cubdb(Support.cubdb_fixture(repo))
    # warm the dbs before measuring
    _ = Goblin.get(goblin, 1)
    {goblin, cubdb, Support.num_keys(repo)}
  end,
  before_each: fn {goblin, cubdb, num_keys} ->
    {goblin, cubdb, :rand.uniform(num_keys), num_keys + :rand.uniform(num_keys)}
  end,
  after_scenario: fn {goblin, cubdb, _num_keys} ->
    Support.stop_goblin(goblin)
    Support.stop_cubdb(cubdb)
  end
)
