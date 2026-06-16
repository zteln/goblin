{db, trigger_flush} =
  case System.get_env("GOBLIN_TEST_DIR") do
    nil ->
      & &1

    data_dir ->
      {:ok, db} = Goblin.start_link(data_dir: data_dir)

      {db,
       fn version ->
         Stream.iterate(1, &(&1 + 1))
         |> Stream.chunk_every(10_000)
         |> Enum.take_while(fn chunk ->
           chunk = Enum.map(chunk, fn x -> {x, "v#{version}-#{x}"} end)
           Goblin.put_multi(db, chunk)
           not Goblin.flushing?(db)
         end)
       end}
  end
