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
      max_sst_size: 100 * 512,
      bf_fpp: 0.01,
      bf_bit_array_size: 100
    ]

    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, next_file_f, opts)

    %{disk_table: disk_table, next_file_f: next_file_f}
  end

  test "is iterable", c do
    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(c.disk_table, [5, 25, 1, 1], 1000)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {{1, 0, "v-1"}, iterator} = Goblin.Iterable.next(iterator)
    assert {{5, 4, "v-5"}, iterator} = Goblin.Iterable.next(iterator)
    assert {{25, 24, "v-25"}, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.close(iterator)
  end

  test "can search in SST block spanning many blocks", c do
    opts = [
      level_key: 0,
      compress?: false,
      max_sst_size: 100 * 512,
      bf_fpp: 0.01,
      bf_bit_array_size: 100
    ]

    triple1 = {1, 0, :crypto.strong_rand_bytes(1024)}
    triple2 = {2, 1, :crypto.strong_rand_bytes(1024)}

    data = [triple1, triple2]

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, c.next_file_f, opts)

    assert disk_table.no_blocks > 2

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [1], 2)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {^triple1, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.close(iterator)

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(disk_table, [2], 2)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {^triple2, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.close(iterator)
  end

  test "does not iterate higher than provided sequence number", c do
    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(c.disk_table, [5, 25, 1, 1], 23)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {{1, 0, "v-1"}, iterator} = Goblin.Iterable.next(iterator)
    assert {{5, 4, "v-5"}, iterator} = Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.close(iterator)
  end

  test "closes file handle when error occurs", c do
    Goblin.DiskTables.Handler
    |> expect(:read, fn _handler, _position, _size ->
      {:error, :failed_to_read}
    end)

    assert %Goblin.DiskTables.BinarySearchIterator{} =
             iterator =
             Goblin.DiskTables.BinarySearchIterator.new(c.disk_table, [5, 25, 1, 1], 1000)

    assert iterator = Goblin.Iterable.init(iterator)

    assert {:error, :eof} == Goblin.DiskTables.Handler.read(iterator.handler, 0)

    assert_raise RuntimeError, fn ->
      Goblin.Iterable.next(iterator)
    end

    assert {:error, :einval} == Goblin.DiskTables.Handler.read(iterator.handler, 0)
  end
end
