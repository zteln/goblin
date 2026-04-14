Mix.install([:benchee, :benchee_markdown, :cubdb, {:goblin, path: File.cwd!()}], force: true)

profile? = "--profile" in System.argv()

results_dir = "#{File.cwd!()}/tmp/goblin_benchmark/results"
File.mkdir_p!(results_dir)

fixtures_dir = "#{File.cwd!()}/tmp/goblin_benchmark/fixtures"

File.exists?(fixtures_dir) ||
  raise "Fixtures must exist. Run `elixir bench/create_fix_repos.exs` first."

num_keys = %{
  "1_kb_repo" => max(div(1024, 1024), 1),
  "1_mb_repo" => div(1024 * 1024, 1024),
  "10_mb_repo" => div(10 * 1024 * 1024, 1024),
  "100_mb_repo" => div(100 * 1024 * 1024, 1024),
  "1_gb_repo" => div(1024 * 1024 * 1024, 1024)
}

Benchee.run(
  %{
    "Goblin.get/2" => fn {goblin, _cubdb, key} ->
      Goblin.get(goblin, key)
    end,
    "CubDB.get/2" => fn {_goblin, cubdb, key} ->
      CubDB.get(cubdb, key)
    end
  },
  inputs: %{
    "1kB" => "1_kb_repo",
    "1MB" => "1_mb_repo",
    "10MB" => "10_mb_repo",
    "100MB" => "100_mb_repo",
    "1GB" => "1_gb_repo"
  },
  before_scenario: fn label ->
    {:ok, goblin} = Goblin.start_link(data_dir: Path.join([fixtures_dir, "goblin", label]))
    {:ok, cubdb} = CubDB.start_link(data_dir: Path.join([fixtures_dir, "cubdb", label]))
    # dummy query to make sure both dbs are ready
    _ = Goblin.get(goblin, 1)
    _ = CubDB.get(cubdb, 1)
    {goblin, cubdb, num_keys[label]}
  end,
  before_each: fn {goblin, cubdb, max_key} ->
    key = :rand.uniform(max_key)
    {goblin, cubdb, key}
  end,
  after_scenario: fn {goblin, cubdb, _max_key} ->
    Goblin.stop(goblin)
    CubDB.stop(cubdb)
  end,
  profile_after: if(profile?, do: :tprof, else: false),
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.Markdown, file: Path.join(results_dir, "get.md")}
  ]
)
