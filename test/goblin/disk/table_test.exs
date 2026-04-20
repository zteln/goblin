defmodule Goblin.Disk.TableTest do
  use ExUnit.Case, async: true

  alias Goblin.BloomFilter
  alias Goblin.Disk
  alias Goblin.Disk.Table

  @moduletag :tmp_dir

  setup ctx do
    counter = :counters.new(1, [])

    next_file_f = fn ->
      count = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      file = Path.join(ctx.tmp_dir, "#{count}.goblin")
      {"#{file}.tmp", file}
    end

    opts = [
      level_key: 0,
      compress?: false,
      max_sst_size: :infinity,
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: next_file_f
    ]

    %{opts: opts}
  end

  defp bf_opts, do: [bf_fpp: 0.01, bf_bit_array_size: 100]

  defp triples(range), do: for(n <- range, do: {n, n - 1, "v-#{n}"})

  defp into_table(data, ctx) do
    {:ok, [table]} = Disk.into_table(data, ctx.opts)
    table
  end

  describe "new/3" do
    test "creates a table with an empty bloom filter and nil ranges" do
      table = Table.new("path.goblin", 2, bf_opts())

      assert table.path == "path.goblin"
      assert table.level_key == 2
      assert table.key_range == nil
      assert table.seq_range == nil
      assert table.size == 0
      assert table.no_blocks == 0
      assert %BloomFilter{} = table.bloom_filter
    end
  end

  describe "add_to_table/3" do
    test "sets ranges, size, and no_blocks on the first triple" do
      table =
        Table.new("path", 0, bf_opts())
        |> Table.add_to_table({:a, 5, "v"}, Table.block_size())

      assert table.key_range == {:a, :a}
      assert table.seq_range == {5, 5}
      assert table.size == Table.block_size()
      assert table.no_blocks == 1
    end

    test "preserves min and extends max across multiple triples" do
      table =
        Table.new("path", 0, bf_opts())
        |> Table.add_to_table({:a, 1, "va"}, Table.block_size())
        |> Table.add_to_table({:b, 2, "vb"}, Table.block_size())
        |> Table.add_to_table({:c, 3, "vc"}, Table.block_size())

      assert table.key_range == {:a, :c}
      assert table.seq_range == {1, 3}
      assert table.size == 3 * Table.block_size()
      assert table.no_blocks == 3
    end

    test "accounts for triples spanning multiple blocks" do
      size = 3 * Table.block_size()

      table =
        Table.new("path", 0, bf_opts())
        |> Table.add_to_table({:a, 0, "v"}, size)

      assert table.size == size
      assert table.no_blocks == 3
    end

    test "adds the key to the bloom filter" do
      table =
        Table.new("path", 0, bf_opts())
        |> Table.add_to_table({:a, 0, "v"}, Table.block_size())

      assert BloomFilter.member?(table.bloom_filter, :a)
      refute BloomFilter.member?(table.bloom_filter, :missing)
    end
  end

  describe "block_size/0" do
    test "returns a positive integer" do
      assert is_integer(Table.block_size())
      assert Table.block_size() > 0
    end
  end

  describe "has_key?/2" do
    test "returns true for keys inside the range", ctx do
      table = into_table(triples(1..10), ctx)

      for n <- 1..10 do
        assert Table.has_key?(table, n)
      end
    end

    test "returns false for keys outside the range", ctx do
      table = into_table(triples(10..20), ctx)

      refute Table.has_key?(table, 9)
      refute Table.has_key?(table, 21)
    end
  end

  describe "within_bounds?/3" do
    test "returns true when bounds overlap the key range", ctx do
      table = into_table(triples(10..20), ctx)

      assert Table.within_bounds?(table, nil, nil)
      assert Table.within_bounds?(table, 15, nil)
      assert Table.within_bounds?(table, nil, 15)
      assert Table.within_bounds?(table, 5, 15)
      assert Table.within_bounds?(table, 10, 20)
    end

    test "returns false when bounds do not overlap", ctx do
      table = into_table(triples(10..20), ctx)

      refute Table.within_bounds?(table, 21, nil)
      refute Table.within_bounds?(table, nil, 9)
      refute Table.within_bounds?(table, 21, 30)
      refute Table.within_bounds?(table, 1, 9)
    end
  end
end
