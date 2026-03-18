defmodule Goblin.DiskTableTest do
  use ExUnit.Case, async: true

  alias Goblin.DiskTable
  alias Goblin.DiskTable.Encoder

  @moduletag :tmp_dir

  @block_size Encoder.sst_block_unit_size()

  setup c do
    counter = :counters.new(1, [])

    next_file_f = fn ->
      count = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      file = Path.join(c.tmp_dir, "#{count}.goblin")
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

  describe "into/2" do
    test "creates a disk table from a stream of triples", c do
      assert {:ok, [disk_table]} = DiskTable.into(triples(1..10), c.opts)

      assert disk_table.level_key == 0
      assert disk_table.key_range == {1, 10}
      assert disk_table.size > 0
      assert File.exists?(disk_table.file)
    end

    test "splits into multiple tables when exceeding max_sst_size", c do
      opts = Keyword.put(c.opts, :max_sst_size, 50 * @block_size)

      assert {:ok, disk_tables} = DiskTable.into(triples(1..100), opts)
      assert length(disk_tables) >= 2
    end

    test "returns {:ok, []} for empty data", c do
      assert {:ok, []} = DiskTable.into([], c.opts)
    end
  end

  describe "from_file/1" do
    test "round-trips a written disk table", c do
      {:ok, [written]} = DiskTable.into(triples(1..50), c.opts)

      assert {:ok, parsed} = DiskTable.from_file(written.file)

      assert parsed.file == written.file
      assert parsed.level_key == written.level_key
      assert parsed.key_range == written.key_range
      assert parsed.seq_range == written.seq_range
      assert parsed.size == written.size
      assert parsed.no_blocks == written.no_blocks
      assert parsed.crc == written.crc
    end
  end

  describe "within_min_max?/2" do
    test "returns true for key inside range and false for key outside", c do
      {:ok, [dt]} = DiskTable.into(triples(10..20), c.opts)

      assert DiskTable.within_min_max?(dt, 15)
      assert DiskTable.within_min_max?(dt, 10)
      assert DiskTable.within_min_max?(dt, 20)
      refute DiskTable.within_min_max?(dt, 9)
      refute DiskTable.within_min_max?(dt, 21)
    end
  end

  describe "bloom_filter_member?/2" do
    test "returns true for keys written to the table", c do
      {:ok, [dt]} = DiskTable.into(triples(1..20), c.opts)

      for n <- 1..20 do
        assert DiskTable.bloom_filter_member?(dt, n)
      end
    end
  end

  describe "within_bounds?/3" do
    test "unbounded query always returns true", c do
      {:ok, [dt]} = DiskTable.into(triples(10..20), c.opts)

      assert DiskTable.within_bounds?(dt, nil, nil)
    end

    test "lower-bounded query checks against table max", c do
      {:ok, [dt]} = DiskTable.into(triples(10..20), c.opts)

      assert DiskTable.within_bounds?(dt, 15, nil)
      assert DiskTable.within_bounds?(dt, 20, nil)
      refute DiskTable.within_bounds?(dt, 21, nil)
    end

    test "upper-bounded query checks against table min", c do
      {:ok, [dt]} = DiskTable.into(triples(10..20), c.opts)

      assert DiskTable.within_bounds?(dt, nil, 15)
      assert DiskTable.within_bounds?(dt, nil, 10)
      refute DiskTable.within_bounds?(dt, nil, 9)
    end

    test "fully bounded query checks overlap", c do
      {:ok, [dt]} = DiskTable.into(triples(10..20), c.opts)

      assert DiskTable.within_bounds?(dt, 5, 15)
      assert DiskTable.within_bounds?(dt, 15, 25)
      assert DiskTable.within_bounds?(dt, 10, 20)
      refute DiskTable.within_bounds?(dt, 1, 9)
      refute DiskTable.within_bounds?(dt, 21, 30)
    end
  end
end
