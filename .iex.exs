data_dir = System.get_env("GOBLIN_TEST_DIR")

{:ok, db} = Goblin.start_link(data_dir: data_dir)

shuffler = fn range, version ->
  range
  |> Enum.shuffle()
  |> Enum.map(fn n -> {n, "v#{version}-#{n}"} end)
end

put_small = fn ->
  data = shuffler.(1..1000, 1)

  :timer.tc(
    fn ->
      Goblin.put_multi(db, data)
    end,
    :millisecond
  )
end

put_medium = fn ->
  data = shuffler.(1..10_000, 2)

  :timer.tc(
    fn ->
      Goblin.put_multi(db, data)
    end,
    :millisecond
  )
end

put_large = fn ->
  data = shuffler.(1..100_000, 3)

  :timer.tc(
    fn ->
      Goblin.put_multi(db, data)
    end,
    :millisecond
  )
end

put_huge = fn version ->
  data = shuffler.(1..1_000_000, version)

  :timer.tc(
    fn ->
      Goblin.put_multi(db, data)
    end,
    :millisecond
  )
end

put_range = fn min, max ->
  data = shuffler.(min..max, 5)

  :timer.tc(
    fn ->
      Goblin.put_multi(db, data)
    end,
    :millisecond
  )
end

view_ssts = fn ->
  :ets.match_object(Goblin.Store, {:_, {:_, :_}, :_})
  |> Enum.map(fn {f, kr, sst} -> {f, kr, sst.level_key, sst.seq_range} end)
  |> Enum.sort_by(fn {_, {s, _}, _, _} -> s end)
  |> dbg(limit: :infinity)
end
