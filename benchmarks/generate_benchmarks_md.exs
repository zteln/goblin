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
  {"scan", "scan"}
]

header = """
# Benchmarks

Goblin v#{version} vs CubDB — #{Date.utc_today()}
"""

body =
  for {name, file} <- sections, into: "" do
    path = Path.join(results_dir, "#{file}.md")
    content = File.read!(path)
    "\n## #{name}\n\n#{content}\n"
  end

File.write!("BENCHMARKS.md", header <> body)
IO.puts("Generated BENCHMARKS.md")
