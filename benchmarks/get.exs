Mix.install([:benchee, {:goblin, path: "#{File.cwd!()}"}])

defmodule StableState do
  def wait_until_stable_state(db, retries \\ 3)
  def wait_until_stable_state(_db, 0), do: :ok

  def wait_until_stable_state(db, retries) do
    Process.sleep(100)

    if Goblin.is_flushing(db) or Goblin.is_compacting(db) do
      wait_until_stable_state(db, retries)
    else
      wait_until_stable_state(db, retries - 1)
    end
  end
end

tmp_dir = "/tmp/goblin_benchmark/"
output_dir = Path.join(File.cwd!(), "benchmark_output")
File.exists?(output_dir) || File.mkdir(output_dir)

File.rm_rf!(tmp_dir)
File.mkdir_p!(tmp_dir)

benchee_runner = fn fname, arg ->
  output_file = "#{fname}_#{DateTime.utc_now() |> DateTime.to_unix()}.benchee"
  catch_all_file = "#{fname}_*.benchee"

  Benchee.run(
    arg,
    inputs: %{
      "small: 1K" => 1000,
      "medium: 10K" => 10_000,
      "large: 100K" => 100_000,
      "huge: 1M" => 1_000_000
    },
    before_scenario: fn max ->
      data = Enum.map(1..max, fn n -> {n, "v-#{n}"} end)
      random = :erlang.unique_integer([:positive])
      db_dir = Path.join(tmp_dir, to_string(random))
      {:ok, db} = Goblin.start_link(db_dir: db_dir)
      Goblin.put_multi(db, data)
      StableState.wait_until_stable_state(db)
      {db, db_dir, max}
    end,
    before_each: fn {db, _db_dir, max} ->
      keys = Enum.take_random(1..max, :rand.uniform(500)) |> Enum.shuffle()
      range = {Enum.min(keys), Enum.max(keys)}
      {db, keys, range}
    end,
    after_scenario: fn {db, db_dir, _} ->
      Supervisor.stop(db, :normal)
      File.rm_rf!(db_dir)
    end,
    save: [path: Path.join(output_dir, output_file)]
  )

  Benchee.report(load: Path.join(output_dir, catch_all_file))
end

benchee_runner.("get", %{
  "get/2" => fn {db, [key | _], _} ->
    Goblin.get(db, key)
  end
})

benchee_runner.("get_multi", %{
  "get_multi/2" => fn {db, keys, _} ->
    Goblin.get_multi(db, keys)
  end
})

benchee_runner.("select-all", %{
  "select/2 all" => fn {db, _, _} ->
    Goblin.select(db) |> Stream.run()
  end
})

benchee_runner.("select", %{
  "select/2" => fn {db, _, {min, max}} ->
    Goblin.select(db, min: min, max: max) |> Stream.run()
  end
})
