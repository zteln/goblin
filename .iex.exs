dir = "/tmp/sea_goat_test/"
# File.rm_rf!(dir)
# File.mkdir(dir)

{:ok, _db} = SeaGoat.start_link(dir: dir, limit: 100, sync_interval: 50)

timer_f = fn ->
  :timer.tc(
    fn ->
      for n <- 1..500 do
        SeaGoat.put(:sea_goat_writer, n, "v-#{n}")
      end
    end,
    :millisecond
  )
end
