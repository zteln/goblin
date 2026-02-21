defmodule CreateRepo do
  @repos_dir "#{File.cwd!()}/tmp/goblin_benchmark/fixtures"
  @value_size 1024
  @batch_size 1000

  def run(label) do
    total_size = total_size(label)
    dir = dir(label)
    num_keys = max(div(total_size, @value_size), 1)

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    IO.puts("Creating #{label} repo at #{dir} (#{num_keys} keys, #{total_size} bytes total)...")

    {:ok, db} = Goblin.start_link(data_dir: dir, name: :"bench_#{label}")

    1..num_keys
    |> Stream.chunk_every(@batch_size)
    |> Enum.each(fn batch ->
      pairs = Enum.map(batch, fn i -> {i, :crypto.strong_rand_bytes(@value_size)} end)
      Goblin.put_multi(db, pairs)
    end)

    wait_until_stable(db)
    Goblin.stop(db)

    IO.puts("  Done: #{label}")
  end

  defp wait_until_stable(db, retries \\ 3)
  defp wait_until_stable(_db, 0), do: :ok

  defp wait_until_stable(db, retries) do
    Process.sleep(100)

    if Goblin.flushing?(db) or Goblin.compacting?(db) do
      wait_until_stable(db, retries)
    else
      wait_until_stable(db, retries - 1)
    end
  end

  defp dir("1kB"), do: Path.join(@repos_dir, "1_kb_repo")
  defp dir("1MB"), do: Path.join(@repos_dir, "1_mb_repo")
  defp dir("10MB"), do: Path.join(@repos_dir, "10_mb_repo")
  defp dir("100MB"), do: Path.join(@repos_dir, "100_mb_repo")
  defp dir("1GB"), do: Path.join(@repos_dir, "1_gb_repo")

  defp total_size("1kB"), do: 1024
  defp total_size("1MB"), do: 1024 * 1024
  defp total_size("10MB"), do: 10 * 1024 * 1024
  defp total_size("100MB"), do: 100 * 1024 * 1024
  defp total_size("1GB"), do: 1024 * 1024 * 1024
end

repos_dir = "#{File.cwd!()}/tmp/goblin_benchmark/fixtures"
File.rm_rf!(repos_dir)
File.mkdir_p!(repos_dir)

for label <- ~w(1kB 1MB 10MB 100MB 1GB),
    do: CreateRepo.run(label)
