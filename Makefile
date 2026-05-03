.PHONY: benchmarks release

test:
	mix test --include property_tests

benchmarks: test
	elixir benchmarks/create_fix_repos.exs
	elixir benchmarks/put.exs
	elixir benchmarks/put_multi.exs
	elixir benchmarks/get.exs
	elixir benchmarks/get_multi.exs
	elixir benchmarks/scan.exs
	elixir benchmarks/generate_benchmarks_md.exs

release: benchmarks
	mix hex.publish
