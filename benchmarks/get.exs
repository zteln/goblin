fixture_dir = Path.join([File.cwd!(), "tmp", "goblin_benchmark", "fixtures"])

sizes = [
  {"1MB", "1_mb_repo", 1024},
  {"10MB", "10_mb_repo", 10 * 1024},
  {"100MB", "100_mb_repo", 100 * 1024},
  {"1GB", "1_gb_repo", 1024 * 1024}
]

inputs =
  for {name, loc, size} <- sizes, into: %{} do
    dir = Path.join(fixture_dir, loc)
    {:ok, db} = Goblin.start(data_dir: dir, name: :"goblin_#{name}")
    {name, {db, size}}
  end

Benchee.run(
  %{
    "get/2 (hit)" => fn {db, hit, _miss} ->
      Goblin.get(db, hit)
    end,
    "get/2 (miss)" => fn {db, _hit, miss} ->
      Goblin.get(db, miss)
    end
  },
  inputs: inputs,
  before_each: fn {db, num_keys} ->
    key = :rand.uniform(num_keys)
    {db, key, key + num_keys}
  end,
  profile_after: if("--profile" in System.argv(), do: :tprof, else: false)
)
