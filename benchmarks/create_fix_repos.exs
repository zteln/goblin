Mix.install([{:goblin, path: File.cwd!()}, :cubdb])

defmodule CreateRepo do
  @fixtures_dir "#{File.cwd!()}/tmp/goblin_benchmark/fixtures"
  @value_size 1024
  @batch_size 1000

  @sizes %{
    "1kB" => 1024,
    "1MB" => 1024 * 1024,
    "10MB" => 10 * 1024 * 1024,
    "100MB" => 100 * 1024 * 1024,
    "1GB" => 1024 * 1024 * 1024
  }

  def run(db_name, start, put, stop, label) do
    total_size = @sizes[label]
    num_keys = max(div(total_size, @value_size), 1)
    dir = Path.join([@fixtures_dir, db_name, dir_name(label)])

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    IO.puts("Creating #{db_name}/#{label} (#{num_keys} keys)...")

    {:ok, db} = start.(dir)

    1..num_keys
    |> Stream.chunk_every(@batch_size)
    |> Enum.each(fn batch ->
      pairs = Enum.map(batch, fn i -> {i, :crypto.strong_rand_bytes(@value_size)} end)
      put.(db, pairs)
    end)

    stop.(db)

    IO.puts("  Done: #{db_name}/#{label}")
  end

  def num_keys(label), do: max(div(@sizes[label], @value_size), 1)

  defp dir_name("1kB"), do: "1_kb_repo"
  defp dir_name("1MB"), do: "1_mb_repo"
  defp dir_name("10MB"), do: "10_mb_repo"
  defp dir_name("100MB"), do: "100_mb_repo"
  defp dir_name("1GB"), do: "1_gb_repo"
end

fixtures_dir = "#{File.cwd!()}/tmp/goblin_benchmark/fixtures"
File.rm_rf!(fixtures_dir)
File.mkdir_p!(fixtures_dir)

db_callbacks = [
  {"goblin",
   fn dir -> Goblin.start_link(data_dir: dir) end,
   fn db, pairs -> Goblin.put_multi(db, pairs) end,
   fn db ->
     # wait for flushing and compaction to finish
     wait = fn wait ->
       if Goblin.flushing?(db) or Goblin.compacting?(db) do
         Process.sleep(100)
         wait.(wait)
       end
     end

     wait.(wait)
     Goblin.stop(db)
   end},
  {"cubdb",
   fn dir -> CubDB.start_link(data_dir: dir) end,
   fn db, pairs -> CubDB.put_multi(db, pairs) end,
   fn db -> CubDB.stop(db) end}
]

for {db_name, start, put, stop} <- db_callbacks,
    label <- ~w(1kB 1MB 10MB 100MB 1GB) do
  CreateRepo.run(db_name, start, put, stop, label)
end

IO.puts("\nAll fixtures created.")
