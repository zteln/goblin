defmodule Goblin.DiskTables.StreamIteratorTest do
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
    assert %Goblin.DiskTables.StreamIterator{} =
             iterator = Goblin.DiskTables.StreamIterator.new(c.disk_table.file)

    assert %Goblin.DiskTables.StreamIterator{} =
             iterator = Goblin.Iterable.init(iterator)

    iterator =
      for n <- 1..100, reduce: iterator do
        acc ->
          key = n
          seq = n - 1
          val = "v-#{n}"
          assert {{^key, ^seq, ^val}, iterator} = Goblin.Iterable.next(acc)
          iterator
      end

    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.deinit(iterator)
  end

  test "does not iterate past provided sequence number", c do
    assert %Goblin.DiskTables.StreamIterator{} =
             iterator = Goblin.DiskTables.StreamIterator.new(c.disk_table.file, 25)

    assert %Goblin.DiskTables.StreamIterator{} =
             iterator = Goblin.Iterable.init(iterator)

    iterator =
      for n <- 1..26, reduce: iterator do
        acc ->
          key = n
          seq = n - 1
          val = "v-#{n}"
          assert {{^key, ^seq, ^val}, iterator} = Goblin.Iterable.next(acc)
          iterator
      end

    assert :ok == Goblin.Iterable.next(iterator)
    assert :ok == Goblin.Iterable.deinit(iterator)
  end

  test "closes handler upom error", c do
    Goblin.DiskTables.Handler
    |> expect(:read, fn _handler, _size ->
      {:error, :failed_to_read}
    end)

    assert %Goblin.DiskTables.StreamIterator{} =
             iterator = Goblin.DiskTables.StreamIterator.new(c.disk_table.file, 25)

    assert %Goblin.DiskTables.StreamIterator{} =
             iterator = Goblin.Iterable.init(iterator)

    assert {:error, :eof} == Goblin.DiskTables.Handler.read(iterator.handler, 0, 0)

    assert_raise RuntimeError, fn ->
      Goblin.Iterable.next(iterator)
    end

    assert {:error, :einval} == Goblin.DiskTables.Handler.read(iterator.handler, 0, 0)
  end
end
