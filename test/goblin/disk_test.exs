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
end
