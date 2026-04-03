Mix.install([:benchee, :benchee_markdown, :cubdb, {:goblin, path: File.cwd!()}])

results_dir = "#{File.cwd!()}/tmp/goblin_benchmark/results"
File.mkdir_p!(results_dir)

goblin_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put_multi/goblin"
cubdb_dir = "#{File.cwd!()}/tmp/goblin_benchmark/put_multi/cubdb"

Benchee.run(
  %{
    "Goblin.put_multi/2" => fn {goblin, _cubdb, pairs} ->
      Goblin.put_multi(goblin, pairs)
    end,
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

    {:ok, goblin} = Goblin.start_link(data_dir: goblin_dir)

    {:ok, cubdb} =
      CubDB.start_link(
        data_dir: cubdb_dir,
        auto_compact: false,
        auto_file_sync: false
      )

    Process.unlink(cubdb)
    {goblin, cubdb, size}
  end,
  before_each: fn {goblin, cubdb, size} ->
    pairs =
      for _ <- 1..size do
        {:rand.uniform(100_000), :crypto.strong_rand_bytes(1024)}
      end

    {goblin, cubdb, pairs}
  end,
  after_scenario: fn {goblin, cubdb, _size} ->
    Goblin.stop(goblin)
    CubDB.stop(cubdb)
    File.rm_rf!(goblin_dir)
    File.rm_rf!(cubdb_dir)
  end,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.Markdown, file: Path.join(results_dir, "put_multi.md")}
  ]
)
