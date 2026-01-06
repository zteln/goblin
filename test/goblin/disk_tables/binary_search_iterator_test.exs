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
      bf_fpp: 0.01
    ]

    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, next_file_f, opts)

    %{disk_table: disk_table}
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
