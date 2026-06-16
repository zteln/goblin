results_dir = "#{File.cwd!()}/tmp/goblin_benchmark/results"

version =
  "mix.exs"
  |> File.read!()
  |> then(&Regex.run(~r/version: "(.+?)"/, &1))
  |> List.last()

sections = [
  {"put", "put"},
  {"put_multi", "put_multi"},
  {"get", "get"},
  {"get_multi", "get_multi"},
  {"scan", "scan"},
  {"mixed", "mixed"}
]

header = """
# Benchmarks

Goblin v#{version} — #{Date.utc_today()}

Run with `make benchmarks` (Goblin only) or `make benchmarks CUBDB=1` to include
the CubDB comparison. The `get`/`get_multi` tables report both read hits and
misses; `mixed` reports a mixed read/write workload at varying ratios.

The benchmarks are:

- **put** / **put_multi** — single and batch writes across value sizes (1 B–100 MB) and batch sizes (10–100k).
- **get** / **get_multi** — single and batch reads across dataset sizes (1 kB–1 GB), reporting both read **hits** and **misses** (the bloom-filter reject path).
- **scan** — bounded range queries across dataset sizes.
- **mixed** — a mixed read/write workload at varying ratios (95% / 50% reads) against a warm dataset.
"""

body =
  for {name, file} <- sections, into: "" do
    path = Path.join(results_dir, "#{file}.md")

    if File.exists?(path) do
      content = File.read!(path)
      "\n## #{name}\n\n#{content}\n"
    else
      ""
    end
  end

File.write!("BENCHMARKS.md", header <> body)
IO.puts("Generated BENCHMARKS.md")
