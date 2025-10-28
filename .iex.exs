db_dir = "/tmp/goblin_test/"
# File.rm_rf!(dir)
# File.mkdir(dir)

{:ok, db} = Goblin.start_link(db_dir: db_dir, sync_interval: 50)

shuffler = fn range, version ->
  range
  |> Enum.shuffle()
  |> Enum.map(fn n -> {n, "v#{version}-#{n}"} end)
end

small = shuffler.(1..1000, 1)
medium = shuffler.(1..10_000, 2)
large = shuffler.(1..100_000, 3)
huge = shuffler.(1..1_000_000, 4)

put_small = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, small)
    end,
    :millisecond
  )
end

put_medium = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, medium)
    end,
    :millisecond
  )
end

put_large = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, large)
    end,
    :millisecond
  )
end

put_huge = fn ->
  :timer.tc(
    fn ->
      Goblin.put_multi(db, huge)
    end,
    :millisecond
  )
end
