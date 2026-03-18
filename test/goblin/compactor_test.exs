defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true

  alias Goblin.{Compactor, DiskTable}

  @moduletag :tmp_dir

  @opts [
    flush_level_file_limit: 4,
    level_base_size: 256,
    level_size_multiplier: 10
  ]

  defp new_compactor(overrides \\ []) do
    Compactor.new(Keyword.merge(@opts, overrides))
  end

  defp new_compactor_with_disk_opts(c, overrides \\ []) do
    Compactor.new(Keyword.merge(@opts ++ disk_table_opts(c), overrides))
  end

  defp fake_disk_table(attrs) do
    struct!(
      %DiskTable{
        file: "default.goblin",
        level_key: 0,
        key_range: {0, 100},
        seq_range: {1, 1},
        size: 0
      },
      attrs
    )
  end

  defp disk_table_opts(c) do
    counter = Map.get(c, :counter, :counters.new(1, []))

    next_file_f = fn ->
      count = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      file = Path.join(c.tmp_dir, "#{count}.goblin")
      {"#{file}.tmp", file}
    end

    [
      max_sst_size: :infinity,
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: next_file_f
    ]
  end

  defp create_disk_table(c, level_key, triples) do
    opts =
      [level_key: level_key, compress?: false] ++ disk_table_opts(c)

    {:ok, [disk_table]} = DiskTable.into(triples, opts)
    disk_table
  end

  describe "new/1" do
    test "creates a compactor with empty levels and queue" do
      compactor = new_compactor()

      assert compactor.levels == %{}
      assert compactor.ref == nil
      assert :queue.is_empty(compactor.queue)
    end
  end

  describe "put_into_level/2" do
    test "adds a disk table to the correct level" do
      dt = fake_disk_table(level_key: 0, file: "a.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt)

      assert [^dt] = compactor.levels[0]
    end

    test "accumulates multiple tables in the same level" do
      dt1 = fake_disk_table(level_key: 1, file: "a.goblin")
      dt2 = fake_disk_table(level_key: 1, file: "b.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)

      assert length(compactor.levels[1]) == 2
    end
  end

  describe "remove_from_level/2" do
    test "removes a disk table by file path" do
      dt1 = fake_disk_table(level_key: 0, file: "a.goblin")
      dt2 = fake_disk_table(level_key: 0, file: "b.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)
        |> Compactor.remove_from_level(dt1)

      assert [^dt2] = compactor.levels[0]
    end
  end

  describe "push/1" do
    test "returns :noop when no levels overflow" do
      dt = fake_disk_table(level_key: 0, file: "a.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt)

      assert {:noop, _compactor} = Compactor.push(compactor)
    end

    test "returns {:compact, level_key, _} when level 0 exceeds file limit" do
      compactor =
        Enum.reduce(1..4, new_compactor(), fn i, acc ->
          dt = fake_disk_table(level_key: 0, file: "#{i}.goblin", key_range: {i, i})
          Compactor.put_into_level(acc, dt)
        end)

      assert {:compact, 0, _compactor} = Compactor.push(compactor)
    end

    test "returns :noop when ref is set even if level overflows" do
      compactor =
        Enum.reduce(1..4, new_compactor(), fn i, acc ->
          dt = fake_disk_table(level_key: 0, file: "#{i}.goblin", key_range: {i, i})
          Compactor.put_into_level(acc, dt)
        end)
        |> Compactor.set_ref(make_ref())

      assert {:noop, _compactor} = Compactor.push(compactor)
    end
  end

  describe "pop/1" do
    test "returns :noop on empty queue" do
      compactor = new_compactor()

      assert {:noop, _compactor} = Compactor.pop(compactor)
    end

    test "dequeues the next level key" do
      # Build an overflowing compactor, push to enqueue, then consume with pop
      compactor =
        Enum.reduce(1..4, new_compactor(), fn i, acc ->
          dt = fake_disk_table(level_key: 0, file: "#{i}.goblin", key_range: {i, i})
          Compactor.put_into_level(acc, dt)
        end)

      # push enqueues and immediately dequeues (ref is nil), so we set ref first
      # to force queuing, then clear ref and pop
      compactor = Compactor.set_ref(compactor, make_ref())
      {:noop, compactor} = Compactor.push(compactor)
      compactor = Compactor.set_ref(compactor, nil)

      assert {:compact, 0, _compactor} = Compactor.pop(compactor)
    end
  end

  describe "set_ref/2" do
    test "sets and clears the ref" do
      compactor = new_compactor()
      ref = make_ref()

      compactor = Compactor.set_ref(compactor, ref)
      assert compactor.ref == ref

      compactor = Compactor.set_ref(compactor, nil)
      assert compactor.ref == nil
    end
  end

  describe "push/1 size-based overflow" do
    test "returns {:compact, level_key, _} when non-level-0 exceeds size limit" do
      # level_base_size is 256, level_size_multiplier is 10
      # level 1 threshold: 256 * 10^0 = 256 bytes
      dt1 = fake_disk_table(level_key: 1, file: "a.goblin", size: 150)
      dt2 = fake_disk_table(level_key: 1, file: "b.goblin", size: 150)

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)

      assert {:compact, 1, _compactor} = Compactor.push(compactor)
    end
  end

  describe "compact/2" do
    test "returns {:ok, [], []} when source level is empty", c do
      # Simulate a stale queue entry: level 0 is empty, level 1 has tables
      dt = create_disk_table(c, 1, [{1, 0, "v1"}, {2, 1, "v2"}])

      compactor =
        new_compactor_with_disk_opts(c)
        |> Compactor.put_into_level(dt)

      assert {:ok, [], []} = Compactor.compact(compactor, 0)
    end

    test "merges level 0 sources into level 1", c do
      counter = :counters.new(1, [])
      c = Map.put(c, :counter, counter)

      dt1 = create_disk_table(c, 0, [{1, 0, "a"}, {3, 1, "c"}])
      dt2 = create_disk_table(c, 0, [{2, 2, "b"}, {4, 3, "d"}])

      compactor =
        new_compactor_with_disk_opts(c)
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)

      assert {:ok, new_tables, old_tables} = Compactor.compact(compactor, 0)

      assert length(new_tables) >= 1
      assert length(old_tables) == 2

      # Verify the new tables are level 1
      assert Enum.all?(new_tables, &(&1.level_key == 1))

      # Verify old tables match what was compacted
      old_files = Enum.map(old_tables, & &1.file) |> Enum.sort()
      expected_files = Enum.map([dt1, dt2], & &1.file) |> Enum.sort()
      assert old_files == expected_files

      # Read back the merged data to verify correctness
      [merged] = new_tables

      data =
        Goblin.Iterator.linear_stream(fn ->
          DiskTable.StreamIterator.new(merged)
        end)
        |> Enum.to_list()

      assert [{1, 0, "a"}, {2, 2, "b"}, {3, 1, "c"}, {4, 3, "d"}] == data
    end
  end
end
