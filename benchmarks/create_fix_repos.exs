fixture_dir = Path.join([File.cwd!(), "tmp", "goblin_benchmark", "fixtures"])

wait_idle = fn db, wait_idle ->
  if Goblin.flushing?(db) or Goblin.compacting?(db) do
    Process.sleep(100)
    wait_idle.(db, wait_idle)
  end
end

[
  {"1_mb_repo", 1024},
  {"10_mb_repo", 10 * 1024},
  {"100_mb_repo", 100 * 1024},
  {"1_gb_repo", 1024 * 1024}
]
|> Enum.each(fn {dir, num_keys} ->
  IO.puts("Inserting #{num_keys} keys...")
  dir = Path.join(fixture_dir, dir)
  {:ok, db} = Goblin.start(data_dir: dir)

  1..num_keys
  |> Stream.chunk_every(1_000)
  |> Enum.each(fn keys ->
    # only insert even integer keys
    pairs = Enum.map(keys, &{&1 * 2, :crypto.strong_rand_bytes(1024)})
    Goblin.put_multi(db, pairs)
  end)

  wait_idle.(db, wait_idle)
  Goblin.stop(db)
end)
