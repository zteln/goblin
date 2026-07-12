tmp_dir = Path.join([File.cwd!(), "tmp", "goblin_benchmark", "put"])
if File.exists?(tmp_dir), do: File.rm_rf!(tmp_dir)

sizes = %{
  "1B" => 1,
  "1kB" => 1024,
  "1MB" => 1024 * 1024,
  "10MB" => 10 * 1024 * 1024,
  "100MB" => 100 * 1024 * 1024
}

inputs =
  for {size, no_bytes} <- sizes, into: %{} do
    dir = Path.join(tmp_dir, size)
    {:ok, db} = Goblin.start(data_dir: dir, name: :"goblin_#{size}")
    {size, {db, no_bytes}}
  end

Benchee.run(
  %{
    "put/2" => fn {db, key, val} ->
      Goblin.put(db, key, val)
    end
  },
  inputs: inputs,
  before_each: fn {db, no_bytes} ->
    key = :rand.uniform(100_000)
    val = :crypto.strong_rand_bytes(no_bytes)
    {db, key, val}
  end,
  profile_after: if("--profile" in System.argv(), do: :tprof, else: false)
)

File.rm_rf!(tmp_dir)
