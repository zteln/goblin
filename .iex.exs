db_dir = "/tmp/goblin_test/"
# File.rm_rf!(dir)
# File.mkdir(dir)

{:ok, db} = Goblin.start_link(db_dir: db_dir, sync_interval: 50)

put_small = fn ->
  :timer.tc(
    fn ->
      for n <- Enum.shuffle(1..1000) do
        Goblin.put(db, n, "v1-#{n}")
      end
    end,
    :millisecond
  )
end

put_medium = fn ->
  :timer.tc(
    fn ->
      for n <- Enum.shuffle(1..10_000) do
        Goblin.put(db, n, "v2-#{n}")
      end
    end,
    :millisecond
  )
end

put_large = fn ->
  :timer.tc(
    fn ->
      for n <- Enum.shuffle(1..100_000) do
        Goblin.put(db, n, "v3-#{n}")
      end
    end,
    :millisecond
  )
end

put_huge = fn ->
  :timer.tc(
    fn ->
      pairs =
        1..1_000_000
        |> Enum.shuffle()
        |> Enum.map(fn n -> {n, "v4-#{n}"} end)

      Goblin.put_multi(db, pairs)
    end,
    :millisecond
  )
end

ltimer_f = fn ->
  :timer.tc(
    fn ->
      for n <- 1..10_000 do
        Goblin.put(db, n, "v-" <> String.duplicate(to_string(n), 511))
      end
    end,
    :millisecond
  )
end
