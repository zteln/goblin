Mix.install([:benchee, :cubdb, {:goblin, path: File.cwd!()}])

goblin_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put/goblin"
cubdb_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put/cubdb"

Benchee.run(
  %{
    "Goblin.put/3" => fn {goblin, _cubdb, key, value} ->
      Goblin.put(goblin, key, value)
    end,
    "CubDB.put/3" => fn {_goblin, cubdb, key, value} ->
      CubDB.put(cubdb, key, value)
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
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
    {:ok, goblin} = Goblin.start_link(data_dir: goblin_dir)

    {:ok, cubdb} =
      CubDB.start_link(
        data_dir: cubdb_dir,
        auto_compact: false,
        auto_file_sync: false
      )

    {goblin, cubdb, size}
  end,
  before_each: fn {goblin, cubdb, size} ->
    key = :rand.uniform(100_000)
    {goblin, cubdb, key, :crypto.strong_rand_bytes(size)}
  end,
  after_scenario: fn {goblin, cubdb, _size} ->
    Goblin.stop(goblin)
    CubDB.stop(cubdb)
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
  end
)
