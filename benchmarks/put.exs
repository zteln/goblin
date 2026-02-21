tmp_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put"

Benchee.run(
  %{
    "Goblin.put/3" => fn {db, key, value} ->
      Goblin.put(db, key, value)
    end
  },
  inputs: %{
    "1B" => 1,
    "1kB" => 1024,
    "1MB" => 1024 * 1024,
    "10MB" => 10 * 1024 * 1024,
    "100MB" => 100 * 1024 * 1024
  },
  before_scenario: fn size ->
    File.rm_rf!(tmp_dir)
    {:ok, db} = Goblin.start_link(data_dir: tmp_dir)
    {db, size}
  end,
  before_each: fn {db, size} ->
    key = :rand.uniform(100_000)
    {db, key, :crypto.strong_rand_bytes(size)}
  end,
  after_scenario: fn {db, _size} ->
    Goblin.stop(db)
    File.rm_rf!(tmp_dir)
  end
)
