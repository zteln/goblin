.PHONY: test benchmarks release

test:
	mix test --include property_tests

# Goblin only by default. Run `make benchmarks CUBDB=1` to also benchmark CubDB.
CUBDB_FLAG := $(if $(CUBDB),--cubdb,)

benchmarks: test
	elixir benchmarks/create_fix_repos.exs $(CUBDB_FLAG)
	elixir benchmarks/put.exs $(CUBDB_FLAG)
	elixir benchmarks/put_multi.exs $(CUBDB_FLAG)
	elixir benchmarks/get.exs $(CUBDB_FLAG)
	elixir benchmarks/get_multi.exs $(CUBDB_FLAG)
	elixir benchmarks/scan.exs $(CUBDB_FLAG)
	elixir benchmarks/mixed.exs $(CUBDB_FLAG)
	elixir benchmarks/generate_benchmarks_md.exs

release: benchmarks
	mix hex.publish
