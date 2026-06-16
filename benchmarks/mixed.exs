Code.require_file("support.exs", __DIR__)
alias Bench.Support

# Mixed read/write workload: a warm dataset, varying read/write ratio.
# Each iteration performs one operation, chosen by a coin flip against the ratio
# (e.g. "95% read" => 95% gets, 5% puts). Measures average op latency under load.
#
# Self-contained: the dataset is built fresh per scenario (no fixtures needed). A
# small mem_limit forces flushes so reads exercise on-disk tables, not just the
# in-memory MemTable.
num_keys = 50_000
mem_limit = 8 * 1024 * 1024
batch = 1000

scratch = "#{File.cwd!()}/tmp/goblin_benchmark/mixed"
goblin_scratch = Path.join(scratch, "goblin")
cubdb_scratch = Path.join(scratch, "cubdb")

populate = fn put ->
  1..num_keys
  |> Stream.chunk_every(batch)
  |> Enum.each(fn keys ->
    put.(Enum.map(keys, fn k -> {k, :crypto.strong_rand_bytes(Support.value_size())} end))
  end)
end

Support.run(
  "mixed",
  %{
    "Goblin mixed" => fn {goblin, _cubdb, key, value, roll, ratio} ->
      if roll <= ratio,
        do: Goblin.get(goblin, key),
        else: Goblin.put(goblin, key, value)
    end
  },
  %{
    "CubDB mixed" => fn {_goblin, cubdb, key, value, roll, ratio} ->
      if roll <= ratio,
        do: CubDB.get(cubdb, key),
        else: CubDB.put(cubdb, key, value)
    end
  },
  inputs: %{
    "95% read" => 0.95,
    "50% read" => 0.50
  },
  before_scenario: fn ratio ->
    File.rm_rf!(scratch)

    goblin = Support.start_goblin(goblin_scratch, mem_limit: mem_limit)
    populate.(fn pairs -> Goblin.put_multi(goblin, pairs) end)
    Support.wait_idle(goblin)

    cubdb =
      if Support.cubdb?() do
        c = Support.start_cubdb(cubdb_scratch)
        populate.(fn pairs -> CubDB.put_multi(c, pairs) end)
        c
      end

    {goblin, cubdb, ratio}
  end,
  before_each: fn {goblin, cubdb, ratio} ->
    key = :rand.uniform(num_keys)
    value = :crypto.strong_rand_bytes(Support.value_size())
    {goblin, cubdb, key, value, :rand.uniform(), ratio}
  end,
  after_scenario: fn {goblin, cubdb, _ratio} ->
    Support.stop_goblin(goblin)
    Support.stop_cubdb(cubdb)
    File.rm_rf!(scratch)
  end
)
