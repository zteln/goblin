tmp_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put_multi"

Benchee.run(
  %{
    "Goblin.put_multi/2" => fn {db, pairs} ->
      Goblin.put_multi(db, pairs)
    end
  },
  inputs: %{
    "10" => 10,
    "100" => 100,
    "1000" => 1000,
    "10000" => 10_000,
    "100000" => 100_000
  },
  before_scenario: fn size ->
    File.rm_rf!(tmp_dir)
    {:ok, db} = Goblin.start_link(data_dir: tmp_dir)
    {db, size}
  end,
  before_each: fn {db, size} ->
    pairs =
      for _ <- 1..size do
        {:rand.uniform(100_000), :crypto.strong_rand_bytes(1024)}
      end

    {db, pairs}
  end,
  after_scenario: fn {db, _size} ->
    Goblin.stop(db)
    File.rm_rf!(tmp_dir)
  end
)
