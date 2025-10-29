db_dir = "/tmp/goblin_test/"

{:ok, db} = Goblin.start_link(db_dir: db_dir, sync_interval: 50)

shuffler = fn range, version ->
  range
  |> Enum.shuffle()
  |> Enum.map(fn n -> {n, "v#{version}-#{n}"} end)
end

put_small = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, shuffler.(1..1000, 1))
    end,
    :millisecond
  )
end

put_medium = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, shuffler.(1..10_000, 2))
    end,
    :millisecond
  )
end

put_large = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, shuffler.(1..100_000, 3))
    end,
    :millisecond
  )
end

put_huge = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, shuffler.(1..1_000_000, 4))
    end,
    :millisecond
  )
end
