defmodule Goblin.Disk.TableTest do
  use ExUnit.Case, async: true

  alias Goblin.BloomFilter
  alias Goblin.Brokerable
  alias Goblin.Disk
  alias Goblin.Disk.{BinarySearchIterator, StreamIterator, Table}
  alias Goblin.Queryable

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

  describe "Brokerable" do
    test "id/1 returns the path" do
      table = Table.new("my-path.goblin", 0, bf_opts())
      assert Brokerable.id(table) == "my-path.goblin"
    end

    test "level_key/1 returns the level" do
      table = Table.new("path", 3, bf_opts())
      assert Brokerable.level_key(table) == 3
    end

    test "remove/1 deletes the backing file", ctx do
      table = into_table(triples(1..5), ctx)
      assert File.exists?(table.path)

      assert :ok = Brokerable.remove(table)
      refute File.exists?(table.path)
    end
  end

  describe "Queryable.has_key?/2" do
    test "returns true for keys inside the range", ctx do
      table = into_table(triples(1..10), ctx)

      for n <- 1..10 do
        assert Queryable.has_key?(table, n)
      end
    end

    test "returns false for keys outside the range", ctx do
      table = into_table(triples(10..20), ctx)

      refute Queryable.has_key?(table, 9)
      refute Queryable.has_key?(table, 21)
    end
  end

  describe "Queryable.search/3" do
    test "returns a BinarySearchIterator when keys are in range", ctx do
      table = into_table(triples(1..10), ctx)
      assert %BinarySearchIterator{} = Queryable.search(table, [5], 100)
    end

    test "returns [] when no requested keys fall inside the range", ctx do
      table = into_table(triples(10..20), ctx)
      assert Queryable.search(table, [5, 25], 100) == []
    end
  end

  describe "Queryable.stream/4" do
    test "returns a StreamIterator when bounds overlap the range", ctx do
      table = into_table(triples(10..20), ctx)

      assert %StreamIterator{} = Queryable.stream(table, nil, nil, 100)
      assert %StreamIterator{} = Queryable.stream(table, 15, nil, 100)
      assert %StreamIterator{} = Queryable.stream(table, nil, 15, 100)
      assert %StreamIterator{} = Queryable.stream(table, 5, 15, 100)
      assert %StreamIterator{} = Queryable.stream(table, 10, 20, 100)
    end

    test "returns [] when bounds do not overlap", ctx do
      table = into_table(triples(10..20), ctx)

      assert Queryable.stream(table, 21, nil, 100) == []
      assert Queryable.stream(table, nil, 9, 100) == []
      assert Queryable.stream(table, 21, 30, 100) == []
      assert Queryable.stream(table, 1, 9, 100) == []
    end
  end
end
