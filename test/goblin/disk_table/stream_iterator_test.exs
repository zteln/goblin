defmodule Goblin.DiskTable.StreamIteratorTest do
  use ExUnit.Case, async: true
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
      max_sst_size: 100 * Goblin.DiskTable.Encoder.sst_block_unit_size(),
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: next_file_f
    ]

    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table]} =
      Goblin.DiskTable.into(data, opts)

    %{disk_table: disk_table, opts: opts}
  end

  test "is iterable", c do
    assert %Goblin.DiskTable.StreamIterator{} =
             iterator = Goblin.DiskTable.StreamIterator.new(c.disk_table)

    assert %Goblin.DiskTable.StreamIterator{} =
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
    assert %Goblin.DiskTable.StreamIterator{} =
             iterator = Goblin.DiskTable.StreamIterator.new(c.disk_table, 25)

    assert %Goblin.DiskTable.StreamIterator{} =
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

  test "handles any term", c do
    opts = [
      level_key: 0,
      compress?: false,
      max_sst_size: :infinity,
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: c.opts[:next_file_f]
    ]

    data =
      StreamData.term()
      |> Stream.take(100)
      |> Enum.with_index(fn key, seq ->
        [val] = StreamData.term() |> Enum.take(1)
        {key, seq, val}
      end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)

    {:ok, [disk_table]} =
      Goblin.DiskTable.into(data, opts)

    iterator = Goblin.DiskTable.StreamIterator.new(disk_table) |> Goblin.Iterable.init()

    Enum.reduce(data, iterator, fn triple, acc ->
      assert {^triple, acc} = Goblin.Iterable.next(acc)
      acc
    end)
  end
end
