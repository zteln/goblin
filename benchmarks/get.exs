repos_dir = "#{File.cwd!()}/tmp/goblin_benchmark/fixtures/"

File.exists?(repos_dir) ||
  raise "fixtures must exist. Run `mix run benchmarks/create_fix_repos.exs`."

Benchee.run(
  %{
    "Goblin.get/3" => fn {db, key} ->
      Goblin.get(db, key)
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
    data_dir = Path.join(repos_dir, label)
    {:ok, db} = Goblin.start_link(data_dir: data_dir)
    db
  end,
  before_each: fn db ->
    key = :rand.uniform(1_000_000)
    {db, key}
  end,
  after_scenario: fn db ->
    Goblin.stop(db)
  end
)
