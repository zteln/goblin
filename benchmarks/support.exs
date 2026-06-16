Mix.install([:benchee, :benchee_markdown, :cubdb, {:goblin, path: File.cwd!()}])

defmodule Bench.Support do
  @moduledoc """
  Shared setup for the Goblin benchmark scripts.

  Every benchmark requires this file as its first line:

      Code.require_file("support.exs", __DIR__)

  which installs the dependencies (once, here) and exposes the helpers below.

  Benchmarks measure Goblin by default. Pass `--cubdb` to also run the CubDB
  comparison jobs (and, for the fixture generator, to build CubDB fixtures).
  Pass `--profile` to profile with `:tprof`.
  """

  @base "#{File.cwd!()}/tmp/goblin_benchmark"
  @value_size 1024

  # Dataset fixtures: display label => on-disk repo directory name.
  @datasets %{
    "1kB" => "1_kb_repo",
    "1MB" => "1_mb_repo",
    "10MB" => "10_mb_repo",
    "100MB" => "100_mb_repo",
    "1GB" => "1_gb_repo"
  }

  @dataset_bytes %{
    "1_kb_repo" => 1024,
    "1_mb_repo" => 1024 * 1024,
    "10_mb_repo" => 10 * 1024 * 1024,
    "100_mb_repo" => 100 * 1024 * 1024,
    "1_gb_repo" => 1024 * 1024 * 1024
  }

  @doc "Whether `--cubdb` was passed; gates CubDB comparison jobs and fixtures."
  def cubdb?, do: "--cubdb" in System.argv()

  @doc "Profiler for Benchee `:profile_after` (`:tprof` when `--profile`, else `false`)."
  def profile_after, do: if("--profile" in System.argv(), do: :tprof, else: false)

  def results_dir do
    dir = Path.join(@base, "results")
    File.mkdir_p!(dir)
    dir
  end

  def fixtures_dir, do: Path.join(@base, "fixtures")

  def require_fixtures! do
    File.exists?(fixtures_dir()) ||
      raise "Fixtures must exist. Run `elixir benchmarks/create_fix_repos.exs` first."
  end

  @doc "Benchee `inputs:` map of display label => repo directory name."
  def dataset_inputs, do: @datasets

  @doc "Number of 1 kB keys populated in a fixture repo, by repo directory name."
  def num_keys(repo), do: max(div(@dataset_bytes[repo], @value_size), 1)

  @doc "Path to a Goblin fixture repo directory."
  def goblin_fixture(repo), do: Path.join([fixtures_dir(), "goblin", repo])

  @doc "Path to a CubDB fixture repo directory."
  def cubdb_fixture(repo), do: Path.join([fixtures_dir(), "cubdb", repo])

  @doc "Benchee `inputs:` map of value-size benchmarks (label => bytes)."
  def value_size_inputs do
    %{
      "1B" => 1,
      "1kB" => 1024,
      "1MB" => 1024 * 1024,
      "10MB" => 10 * 1024 * 1024,
      "100MB" => 100 * 1024 * 1024
    }
  end

  def value_size, do: @value_size

  def start_goblin(dir, opts \\ []) do
    {:ok, pid} = Goblin.start_link([data_dir: dir] ++ opts)
    pid
  end

  @doc "Start a CubDB instance, or return `nil` when `--cubdb` was not passed."
  def start_cubdb(dir, opts \\ []) do
    if cubdb?() do
      {:ok, pid} = CubDB.start_link([data_dir: dir] ++ opts)
      pid
    end
  end

  def stop_goblin(pid), do: Goblin.stop(pid)
  def stop_cubdb(nil), do: :ok
  def stop_cubdb(pid), do: CubDB.stop(pid)

  @doc "Block until Goblin has finished any in-flight flush and compaction."
  def wait_idle(goblin) do
    if Goblin.flushing?(goblin) or Goblin.compacting?(goblin) do
      Process.sleep(100)
      wait_idle(goblin)
    end
  end

  @doc """
  Run a Benchee benchmark named `name`.

  `goblin_jobs` always run; `cubdb_jobs` run only when `--cubdb` was passed.
  `opts` are the remaining Benchee options (`:inputs`, `:before_scenario`, ...);
  the profiler and the console + markdown formatters are merged in here.
  """
  def run(name, goblin_jobs, cubdb_jobs \\ %{}, opts \\ []) do
    jobs = if cubdb?(), do: Map.merge(goblin_jobs, cubdb_jobs), else: goblin_jobs

    opts =
      opts
      |> Keyword.put(:profile_after, profile_after())
      |> Keyword.put(:formatters, [
        Benchee.Formatters.Console,
        {Benchee.Formatters.Markdown, file: Path.join(results_dir(), "#{name}.md")}
      ])

    Benchee.run(jobs, opts)
  end
end
