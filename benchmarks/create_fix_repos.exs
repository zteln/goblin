Code.require_file("support.exs", __DIR__)
alias Bench.Support

defmodule CreateRepo do
  @batch_size 1000

  def run(db_name, start, put, stop, repo) do
    num_keys = Bench.Support.num_keys(repo)
    dir = Path.join([Bench.Support.fixtures_dir(), db_name, repo])

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    IO.puts("Creating #{db_name}/#{repo} (#{num_keys} keys)...")

    {:ok, db} = start.(dir)

    1..num_keys
    |> Stream.chunk_every(@batch_size)
    |> Enum.each(fn batch ->
      pairs = Enum.map(batch, fn i -> {i, :crypto.strong_rand_bytes(Bench.Support.value_size())} end)
      put.(db, pairs)
    end)

    stop.(db)

    IO.puts("  Done: #{db_name}/#{repo}")
  end
end

File.rm_rf!(Support.fixtures_dir())
File.mkdir_p!(Support.fixtures_dir())

goblin = {
  "goblin",
  fn dir -> Goblin.start_link(data_dir: dir) end,
  fn db, pairs -> Goblin.put_multi(db, pairs) end,
  fn db ->
    Support.wait_idle(db)
    Goblin.stop(db)
  end
}

cubdb = {
  "cubdb",
  fn dir -> CubDB.start_link(data_dir: dir) end,
  fn db, pairs -> CubDB.put_multi(db, pairs) end,
  fn db -> CubDB.stop(db) end
}

db_callbacks = if Support.cubdb?(), do: [goblin, cubdb], else: [goblin]

for {db_name, start, put, stop} <- db_callbacks,
    repo <- Map.values(Support.dataset_inputs()) do
  CreateRepo.run(db_name, start, put, stop, repo)
end

IO.puts("\nAll fixtures created.")
