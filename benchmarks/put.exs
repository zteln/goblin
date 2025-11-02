Mix.install([:benchee, {:goblin, path: "#{File.cwd!()}"}])

tmp_dir = "/tmp/goblin_benchmark/"
output_dir = Path.join(File.cwd!(), "benchmark_output")
output_file = "write_#{DateTime.utc_now() |> DateTime.to_unix()}.benchee"
File.exists?(output_dir) || File.mkdir(output_dir)

File.rm_rf!(tmp_dir)
File.mkdir_p!(tmp_dir)

Benchee.run(
  %{
    "put/3" => fn {db, db_dir, input} ->
      Enum.each(input, fn {k, v} -> Goblin.put(db, k, v) end)
      {db, db_dir}
    end,
    "put_multi/2" => fn {db, db_dir, input} ->
      Goblin.put_multi(db, input)
      {db, db_dir}
    end
  },
  inputs: %{
    "small: 1K" => Enum.shuffle(1..1000) |> Enum.map(&{&1, "v-#{&1}"}),
    "medium: 10K" => Enum.shuffle(1..10_000) |> Enum.map(&{&1, "v-#{&1}"}),
    "large: 100K" => Enum.shuffle(1..100_000) |> Enum.map(&{&1, "v-#{&1}"}),
    "huge: 1M" => Enum.shuffle(1..1_000_000) |> Enum.map(&{&1, "v-#{&1}"})
  },
  before_each: fn input ->
    random = :erlang.unique_integer([:positive])
    db_dir = Path.join(tmp_dir, to_string(random))
    {:ok, db} = Goblin.start_link(db_dir: db_dir)
    {db, db_dir, input}
  end,
  after_each: fn {db, db_dir} ->
    Supervisor.stop(db, :normal)
    File.rm_rf!(db_dir)
  end,
  save: [path: Path.join(output_dir, output_file)]
)

Benchee.report(load: Path.join(output_dir, "write_*.benchee"))
