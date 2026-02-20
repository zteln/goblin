defmodule Goblin.DiskTables.BinarySearchIteratorTest do
  use ExUnit.Case, async: true
  use Mimic
  @moduletag :tmp_dir

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
      max_sst_size: :infinity,
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: next_file_f
    ]

    %{opts: opts}
  end

  test "is iterable", c do
    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, c.opts)

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [5, 25, 1, 1], 1000)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {{1, 0, "v-1"}, iterator} = Goblin.Iterable.next(iterator)
    assert {{5, 4, "v-5"}, iterator} = Goblin.Iterable.next(iterator)
    assert {{25, 24, "v-25"}, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.deinit(iterator)
  end

  test "can search in SST block spanning many blocks", c do
    opts = [
      level_key: 0,
      compress?: false,
      max_sst_size: 100 * Goblin.DiskTables.Encoder.sst_block_unit_size(),
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: c.opts[:next_file_f]
    ]

    triple1 =
      {1, 0, :crypto.strong_rand_bytes(3 * Goblin.DiskTables.Encoder.sst_block_unit_size())}

    triple2 =
      {2, 1, :crypto.strong_rand_bytes(5 * Goblin.DiskTables.Encoder.sst_block_unit_size())}

    data = [triple1, triple2]

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, opts)

    assert disk_table.no_blocks > 2

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [1], 2)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {^triple1, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.deinit(iterator)

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [2], 2)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {^triple2, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.deinit(iterator)
  end

  test "keys of same value (i.e. key1 == key2) can be found", c do
    data =
      Stream.cycle([0, 0.0, 1.0, 1])
      |> Stream.take(4)
      |> Enum.with_index(fn key, seq -> {key, seq, "v-#{seq}-#{key}"} end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, c.opts)

    iterator =
      disk_table
      |> Goblin.DiskTables.BinarySearchIterator.new([0], length(data))
      |> Goblin.Iterable.init()

    zero_key_triple = Enum.find(data, fn {key, _seq, _val} -> key == 0.0 end)
    assert {^zero_key_triple, _iterator} = Goblin.Iterable.next(iterator)

    iterator =
      disk_table
      |> Goblin.DiskTables.BinarySearchIterator.new([0.0, 0, 0.0], length(data))
      |> Goblin.Iterable.init()

    zero_key_triple = Enum.find(data, fn {key, _seq, _val} -> key == 0 end)
    assert {^zero_key_triple, _iterator} = Goblin.Iterable.next(iterator)

    iterator =
      disk_table
      |> Goblin.DiskTables.BinarySearchIterator.new([1], length(data))
      |> Goblin.Iterable.init()

    one_key_triple = Enum.find(data, fn {key, _seq, _val} -> key == 1.0 end)
    assert {^one_key_triple, _iterator} = Goblin.Iterable.next(iterator)

    iterator =
      disk_table
      |> Goblin.DiskTables.BinarySearchIterator.new([1.0], length(data))
      |> Goblin.Iterable.init()

    one_key_triple = Enum.find(data, fn {key, _seq, _val} -> key == 1 end)
    assert {^one_key_triple, _iterator} = Goblin.Iterable.next(iterator)
  end

  test "does not iterate higher than provided sequence number", c do
    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, c.opts)

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [5, 25, 1, 1], 23)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {{1, 0, "v-1"}, iterator} = Goblin.Iterable.next(iterator)
    assert {{5, 4, "v-5"}, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.deinit(iterator)
  end

  test "closes file handle when error occurs", c do
    Goblin.DiskTables.Handler
    |> expect(:read, fn _handler, _position, _size ->
      {:error, :failed_to_read}
    end)

    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, c.opts)

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [5, 25, 1, 1], 1000)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {:error, :eof} == Goblin.DiskTables.Handler.read(iterator.handler, 0)

    assert_raise RuntimeError, fn ->
      Goblin.Iterable.next(iterator)
    end

    assert {:error, :einval} == Goblin.DiskTables.Handler.read(iterator.handler, 0)
  end

  test "can handle any term", c do
    data =
      StreamData.term()
      |> Stream.take(100)
      |> Enum.with_index(fn key, seq ->
        [val] = StreamData.term() |> Enum.take(1)
        {key, seq, val}
      end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)

    keys = Enum.map(data, &elem(&1, 0))

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, c.opts)

    iterator =
      disk_table
      |> Goblin.DiskTables.BinarySearchIterator.new(keys, length(data))
      |> Goblin.Iterable.init()

    data
    |> Goblin.TestHelper.uniq_by_value(&elem(&1, 0))
    |> Enum.reduce(iterator, fn triple, acc ->
      assert {^triple, acc} = Goblin.Iterable.next(acc)
      acc
    end)
  end
end
