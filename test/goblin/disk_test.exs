defmodule Goblin.DiskTest do
  use ExUnit.Case, async: true

  alias Goblin.Disk
  alias Goblin.Disk.Table

  @moduletag :tmp_dir

  @block_size Table.block_size()

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
      max_sst_size: 100 * @block_size,
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: next_file_f
    ]

    %{opts: opts}
  end

  defp triples(range) do
    for n <- range, do: {n, n, "v-#{n}"}
  end

  describe "into_table/2" do
    test "creates a disk table from a stream of triples", ctx do
      assert {:ok, [disk_table]} = Disk.into_table(triples(1..10), ctx.opts)

      assert disk_table.level_key == 0
      assert disk_table.key_range == {1, 10}
      assert disk_table.size > 0
      assert File.exists?(disk_table.path)
    end

    test "splits into multiple tables when exceeding max_sst_size", ctx do
      opts = Keyword.put(ctx.opts, :max_sst_size, 50 * @block_size)

      assert {:ok, disk_tables} = Disk.into_table(triples(1..100), opts)
      assert length(disk_tables) >= 2
    end

    test "returns {:ok, []} for empty data", ctx do
      assert {:ok, []} = Disk.into_table([], ctx.opts)
    end
  end

  describe "from_file/1" do
    test "round-trips a written disk table", ctx do
      {:ok, [written]} = Disk.into_table(triples(1..50), ctx.opts)

      assert {:ok, parsed} = Disk.from_file(written.path)

      assert parsed.path == written.path
      assert parsed.level_key == written.level_key
      assert parsed.key_range == written.key_range
      assert parsed.seq_range == written.seq_range
      assert parsed.size == written.size
      assert parsed.no_blocks == written.no_blocks
    end
  end

  describe "stream/4" do
    test "yields all triples when no bounds given", ctx do
      {:ok, [dt]} = Disk.into_table(triples(1..5), ctx.opts)

      result = Disk.stream(dt, nil, nil, 100) |> Enum.to_list()

      assert result == [
               {1, 1, "v-1"},
               {2, 2, "v-2"},
               {3, 3, "v-3"},
               {4, 4, "v-4"},
               {5, 5, "v-5"}
             ]
    end

    test "filters by seq (yields only triples with seq <= max_seq)", ctx do
      {:ok, [dt]} = Disk.into_table(triples(1..5), ctx.opts)

      result = Disk.stream(dt, nil, nil, 3) |> Enum.to_list()

      assert result == [{1, 1, "v-1"}, {2, 2, "v-2"}, {3, 3, "v-3"}]
    end

    test "returns [] when table is out of bounds", ctx do
      {:ok, [dt]} = Disk.into_table(triples(10..20), ctx.opts)

      # table has keys 10..20; asking for [1..5] is out of range
      assert [] == Disk.stream(dt, 1, 5, 100)
    end

    test "streams table when bounds overlap its key_range", ctx do
      {:ok, [dt]} = Disk.into_table(triples(10..20), ctx.opts)

      # bounds [5..15] overlap with table range [10..20]
      result = Disk.stream(dt, 5, 15, 100) |> Enum.to_list()
      # Disk.stream does not clip bounds — it yields the whole table when bounds overlap.
      assert length(result) == 11
    end
  end

  describe "search/3" do
    test "yields only requested keys in ascending order", ctx do
      {:ok, [dt]} = Disk.into_table(triples(1..50), ctx.opts)

      result = Disk.search(dt, [5, 10, 25], 100) |> Enum.to_list()

      assert result == [{5, 5, "v-5"}, {10, 10, "v-10"}, {25, 25, "v-25"}]
    end

    test "skips missing keys", ctx do
      {:ok, [dt]} = Disk.into_table(triples(1..10), ctx.opts)

      # 99 does not exist
      result = Disk.search(dt, [5, 99], 100) |> Enum.to_list()
      assert result == [{5, 5, "v-5"}]
    end

    test "filters by seq", ctx do
      {:ok, [dt]} = Disk.into_table(triples(1..10), ctx.opts)

      # max_seq = 4 excludes keys with seq >= 4
      result = Disk.search(dt, [1, 5], 4) |> Enum.to_list()
      assert result == [{1, 1, "v-1"}]
    end

    test "is lazy: Enum.take/2 returns early without consuming the full stream", ctx do
      {:ok, [dt]} = Disk.into_table(triples(1..100), ctx.opts)

      stream = Disk.search(dt, [5, 10, 50], 1000)

      assert [{5, _, _}] = Enum.take(stream, 1)
    end
  end
end
