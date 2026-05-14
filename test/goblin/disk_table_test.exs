defmodule Goblin.DiskTableTest do
  use ExUnit.Case, async: true

  alias Goblin.DiskTable

  @moduletag :tmp_dir

  setup ctx do
    counter = :counters.new(1, [])

    filer = fn ->
      n = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      Path.join(ctx.tmp_dir, "#{n}.goblin")
    end

    opts = [
      level_key: 0,
      compress?: false,
      max_size: :infinity,
      fpp: 0.01,
      bit_array_size: 100,
      filer: filer
    ]

    %{opts: opts}
  end

  defp triples(range), do: for(n <- range, do: {n, n, "v-#{n}"})

  defp build_one(data, opts) do
    {:ok, [dt]} = DiskTable.build(data, opts)
    dt
  end

  describe "build/2" do
    test "writes a disk table with the expected metadata and a file on disk", ctx do
      assert {:ok, [dt]} = DiskTable.build(triples(1..10), ctx.opts)

      assert dt.level_key == 0
      assert dt.key_range == {1, 10}
      assert dt.seq_range == {1, 10}
      assert dt.size > 0
      assert dt.no_blocks > 0
      assert File.exists?(dt.id)
    end

    test "returns {:ok, []} for an empty stream and writes no files", ctx do
      assert {:ok, []} = DiskTable.build([], ctx.opts)
      assert File.ls!(ctx.tmp_dir) == []
    end

    test "splits into multiple tables when exceeding max_size", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 4 * 1024)

      assert {:ok, dts} = DiskTable.build(triples(1..200), opts)
      assert length(dts) >= 2

      # Tables are written in reverse insertion order; sort by key_range ascending.
      sorted = Enum.sort_by(dts, & &1.key_range)

      sorted
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] ->
        {_, a_max} = a.key_range
        {b_min, _} = b.key_range
        assert a_max < b_min
      end)
    end
  end

  describe "from_file/1" do
    test "round-trips a written table", ctx do
      written = build_one(triples(1..50), ctx.opts)

      assert {:ok, parsed} = DiskTable.from_file(written.id)

      assert parsed.id == written.id
      assert parsed.level_key == written.level_key
      assert parsed.key_range == written.key_range
      assert parsed.seq_range == written.seq_range
      assert parsed.size == written.size
      assert parsed.no_blocks == written.no_blocks
      assert parsed.bloom_filter == written.bloom_filter
    end
  end

  describe "has_key?/2" do
    test "returns true for every key that was written", ctx do
      dt = build_one(triples(1..10), ctx.opts)

      for n <- 1..10 do
        assert DiskTable.has_key?(dt, n)
      end
    end

    test "returns false for keys outside the key_range", ctx do
      dt = build_one(triples(10..20), ctx.opts)

      refute DiskTable.has_key?(dt, 9)
      refute DiskTable.has_key?(dt, 21)
    end

    test "returns false for a never-inserted key inside the key_range", ctx do
      dt = build_one(triples([1, 10, 20]), ctx.opts)

      refute DiskTable.has_key?(dt, 5)
    end
  end

  describe "search/3" do
    test "yields requested keys in the input order", ctx do
      dt = build_one(triples(1..50), ctx.opts)

      result = DiskTable.search(dt, [5, 10, 25], 1000) |> Enum.to_list()

      assert result == [{5, 5, "v-5"}, {10, 10, "v-10"}, {25, 25, "v-25"}]
    end

    test "skips keys that are not in the table", ctx do
      dt = build_one(triples(1..10), ctx.opts)

      result = DiskTable.search(dt, [5, 99], 1000) |> Enum.to_list()

      assert result == [{5, 5, "v-5"}]
    end

    test "filters strictly by seq (s < seq)", ctx do
      dt = build_one(triples(1..10), ctx.opts)

      result = DiskTable.search(dt, [1, 4, 5], 4) |> Enum.to_list()

      assert result == [{1, 1, "v-1"}]
    end

    test "is lazy: Enum.take returns early without consuming the full search", ctx do
      dt = build_one(triples(1..100), ctx.opts)

      stream = DiskTable.search(dt, [5, 10, 50], 1000)

      assert [{5, _, _}] = Enum.take(stream, 1)
    end

    test "finds triples whose values span many physical blocks", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 100 * 1024)

      triple1 = {1, 1, :crypto.strong_rand_bytes(3 * 1024)}
      triple2 = {2, 2, :crypto.strong_rand_bytes(5 * 1024)}

      dt = build_one([triple1, triple2], opts)
      assert dt.no_blocks > 2

      assert DiskTable.search(dt, [1], 1000) |> Enum.to_list() == [triple1]
      assert DiskTable.search(dt, [2], 1000) |> Enum.to_list() == [triple2]
      assert DiskTable.search(dt, [1, 2], 1000) |> Enum.to_list() == [triple1, triple2]
    end
  end

  describe "stream/2" do
    test "yields every triple in stored order when no opts are given", ctx do
      dt = build_one(triples(1..5), ctx.opts)

      assert DiskTable.stream(dt) |> Enum.to_list() == [
               {1, 1, "v-1"},
               {2, 2, "v-2"},
               {3, 3, "v-3"},
               {4, 4, "v-4"},
               {5, 5, "v-5"}
             ]
    end

    test "filters strictly by :seq (s < seq)", ctx do
      dt = build_one(triples(1..5), ctx.opts)

      assert DiskTable.stream(dt, seq: 3) |> Enum.to_list() == [
               {1, 1, "v-1"},
               {2, 2, "v-2"}
             ]
    end

    test "returns [] when :bounds do not overlap the key_range", ctx do
      dt = build_one(triples(10..20), ctx.opts)

      assert DiskTable.stream(dt, bounds: {1, 5}) == []
      assert DiskTable.stream(dt, bounds: {21, 30}) == []
    end

    test "yields the whole table when :bounds overlap the key_range (no clipping)", ctx do
      dt = build_one(triples(10..20), ctx.opts)

      result = DiskTable.stream(dt, bounds: {5, 15}) |> Enum.to_list()
      assert length(result) == 11
      assert hd(result) == {10, 10, "v-10"}
      assert List.last(result) == {20, 20, "v-20"}
    end

    test "is lazy: Enum.take returns early", ctx do
      dt = build_one(triples(1..100), ctx.opts)

      assert [{1, 1, "v-1"}] = Enum.take(DiskTable.stream(dt), 1)
    end
  end
end
