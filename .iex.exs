dir = "/tmp/sea_goat_test/"
File.rm_rf!(dir)
File.mkdir(dir)

{:ok, _db} = SeaGoat.start_link(dir: dir, limit: 10000, sync_interval: 200)

timer_f = fn ->
  :timer.tc(
    fn ->
      for n <- 1..1_000_000 do
        SeaGoat.put(SeaGoat, n, "v-#{n}")
      end
    end,
    :millisecond
  )
end
