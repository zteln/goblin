Code.require_file("support.exs", __DIR__)
alias Bench.Support

Support.require_fixtures!()

batch = 1000

# Fixtures populate keys 1..num_keys; miss keys sit in num_keys+1..2*num_keys.
Support.run(
  "get_multi",
  %{
    "Goblin.get_multi/2 (hit)" => fn {goblin, _cubdb, hits, _misses} ->
      Goblin.get_multi(goblin, hits)
    end,
    "Goblin.get_multi/2 (miss)" => fn {goblin, _cubdb, _hits, misses} ->
      Goblin.get_multi(goblin, misses)
    end
  },
  %{
    "CubDB.get_multi/2 (hit)" => fn {_goblin, cubdb, hits, _misses} ->
      CubDB.get_multi(cubdb, hits)
    end,
    "CubDB.get_multi/2 (miss)" => fn {_goblin, cubdb, _hits, misses} ->
      CubDB.get_multi(cubdb, misses)
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
    hits = for _ <- 1..batch, do: :rand.uniform(num_keys)
    misses = for _ <- 1..batch, do: num_keys + :rand.uniform(num_keys)
    {goblin, cubdb, hits, misses}
  end,
  after_scenario: fn {goblin, cubdb, _num_keys} ->
    Support.stop_goblin(goblin)
    Support.stop_cubdb(cubdb)
  end
)
