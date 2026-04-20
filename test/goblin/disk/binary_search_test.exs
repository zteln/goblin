defmodule Goblin.Disk.BinarySearchTest do
  use ExUnit.Case, async: true

  alias Goblin.Disk
  alias Goblin.Disk.{BinarySearch, Table}
  alias Goblin.FileIO

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

  defp open_io(path), do: FileIO.open!(path, block_size: Table.block_size())

  defp walk(bs, acc \\ []) do
    case BinarySearch.next(bs) do
      {:ok, bs} -> {Enum.reverse(acc), bs}
      {:ok, triple, bs} -> walk(bs, [triple | acc])
    end
  end

  test "walks requested keys in ascending order", ctx do
    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} = Disk.into_table(data, ctx.opts)

    io = open_io(disk_table.path)

    try do
      bs = BinarySearch.new(disk_table, io, [1, 5, 25], 1000)
      {triples, _bs} = walk(bs)

      assert triples == [
               {1, 0, "v-1"},
               {5, 4, "v-5"},
               {25, 24, "v-25"}
             ]
    after
      FileIO.close(io)
    end
  end

  test "can search in SST blocks spanning many physical blocks", ctx do
    opts = [
      level_key: 0,
      compress?: false,
      max_sst_size: 100 * Table.block_size(),
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: ctx.opts[:next_file_f]
    ]

    triple1 = {1, 0, :crypto.strong_rand_bytes(3 * Table.block_size())}
    triple2 = {2, 1, :crypto.strong_rand_bytes(5 * Table.block_size())}

    data = [triple1, triple2]

    {:ok, [disk_table]} = Disk.into_table(data, opts)

    assert disk_table.no_blocks > 2

    io = open_io(disk_table.path)

    try do
      bs = BinarySearch.new(disk_table, io, [1], 2)
      assert {:ok, ^triple1, bs} = BinarySearch.next(bs)
      assert {:ok, _bs} = BinarySearch.next(bs)

      bs = BinarySearch.new(disk_table, io, [2], 2)
      assert {:ok, ^triple2, bs} = BinarySearch.next(bs)
      assert {:ok, _bs} = BinarySearch.next(bs)
    after
      FileIO.close(io)
    end
  end

  test "keys of same value (1 vs 1.0) are found by strict equality", ctx do
    data =
      Stream.cycle([0, 0.0, 1.0, 1])
      |> Stream.take(4)
      |> Enum.with_index(fn key, seq -> {key, seq, "v-#{seq}-#{key}"} end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)

    {:ok, [disk_table]} = Disk.into_table(data, ctx.opts)

    io = open_io(disk_table.path)

    try do
      bs = BinarySearch.new(disk_table, io, [0], length(data))
      zero_key_triple = Enum.find(data, fn {key, _seq, _val} -> key == 0.0 end)
      assert {:ok, ^zero_key_triple, _bs} = BinarySearch.next(bs)

      bs = BinarySearch.new(disk_table, io, [0.0], length(data))
      zero_float_triple = Enum.find(data, fn {key, _seq, _val} -> key == 0 end)
      assert {:ok, ^zero_float_triple, _bs} = BinarySearch.next(bs)

      bs = BinarySearch.new(disk_table, io, [1], length(data))
      one_key_triple = Enum.find(data, fn {key, _seq, _val} -> key == 1.0 end)
      assert {:ok, ^one_key_triple, _bs} = BinarySearch.next(bs)

      bs = BinarySearch.new(disk_table, io, [1.0], length(data))
      one_float_triple = Enum.find(data, fn {key, _seq, _val} -> key == 1 end)
      assert {:ok, ^one_float_triple, _bs} = BinarySearch.next(bs)
    after
      FileIO.close(io)
    end
  end

  test "does not iterate higher than provided sequence number", ctx do
    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} = Disk.into_table(data, ctx.opts)

    io = open_io(disk_table.path)

    try do
      bs = BinarySearch.new(disk_table, io, [1, 5, 25], 23)
      {triples, _bs} = walk(bs)

      # keys 25 has seq 24 which is >= 23, so it's filtered out
      assert triples == [{1, 0, "v-1"}, {5, 4, "v-5"}]
    after
      FileIO.close(io)
    end
  end

  test "can handle any term as a key", ctx do
    data =
      StreamData.term()
      |> Stream.take(100)
      |> Enum.with_index(fn key, seq ->
        [val] = StreamData.term() |> Enum.take(1)
        {key, seq, val}
      end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)

    keys = data |> Enum.map(&elem(&1, 0)) |> Enum.dedup()

    {:ok, [disk_table]} = Disk.into_table(data, ctx.opts)

    io = open_io(disk_table.path)

    try do
      bs = BinarySearch.new(disk_table, io, keys, length(data))
      {triples, _bs} = walk(bs)

      expected = Goblin.TestHelper.uniq_by_value(data, &elem(&1, 0))
      assert triples == expected
    after
      FileIO.close(io)
    end
  end
end
