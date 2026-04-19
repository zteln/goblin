defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true

  alias Goblin.{Compactor, Disk}
  alias Goblin.Disk.{StreamIterator, Table}

  @moduletag :tmp_dir

  @opts [
    flush_level_file_limit: 4,
    level_base_size: 256,
    level_size_multiplier: 10
  ]

  defp new_compactor(overrides \\ []) do
    Compactor.new(Keyword.merge(@opts, overrides))
  end

  defp new_compactor_with_disk_opts(ctx, overrides) do
    Compactor.new(Keyword.merge(@opts ++ disk_table_opts(ctx), overrides))
  end

  defp fake_disk_table(attrs) do
    struct!(
      %Table{
        path: "default.goblin",
        level_key: 0,
        key_range: {0, 100},
        seq_range: {1, 1},
        size: 0
      },
      attrs
    )
  end

  defp disk_table_opts(ctx) do
    counter = Map.get(ctx, :counter, :counters.new(1, []))

    next_file_f = fn ->
      count = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      file = Path.join(ctx.tmp_dir, "#{count}.goblin")
      {"#{file}.tmp", file}
    end

    [
      max_sst_size: :infinity,
      bf_fpp: 0.01,
      bf_bit_array_size: 100,
      next_file_f: next_file_f
    ]
  end

  defp create_disk_table(ctx, level_key, triples) do
    opts =
      [level_key: level_key, compress?: false] ++ disk_table_opts(ctx)

    {:ok, [disk_table]} = Disk.into_table(triples, opts)
    disk_table
  end

  describe "new/1" do
    test "creates a compactor with empty levels and queue" do
      compactor = new_compactor()

      assert compactor.levels == %{}
      assert :queue.is_empty(compactor.queue)
    end
  end

  describe "put_into_level/2" do
    test "adds a disk table to the correct level" do
      dt = fake_disk_table(level_key: 0, path: "a.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt)

      assert [^dt] = compactor.levels[0]
    end

    test "accumulates multiple tables in the same level" do
      dt1 = fake_disk_table(level_key: 1, path: "a.goblin")
      dt2 = fake_disk_table(level_key: 1, path: "b.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)

      assert length(compactor.levels[1]) == 2
    end
  end

  describe "next/1" do
    test "returns :noop when no levels overflow" do
      dt = fake_disk_table(level_key: 0, path: "a.goblin")

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt)

      assert {:noop, _compactor} = Compactor.next(compactor)
    end

    test "returns {:compact, ...} when level 0 exceeds file limit" do
      compactor =
        Enum.reduce(1..4, new_compactor(), fn i, acc ->
          dt = fake_disk_table(level_key: 0, path: "#{i}.goblin", key_range: {i, i})
          Compactor.put_into_level(acc, dt)
        end)

      assert {:compact, 1, sources, _targets, _filter?, _compactor} = Compactor.next(compactor)
      assert length(sources) == 4
    end

    test "returns {:compact, ...} when non-level-0 exceeds size limit" do
      # level_base_size is 256, level_size_multiplier is 10
      # level 1 threshold: 256 * 10^0 = 256 bytes
      dt1 = fake_disk_table(level_key: 1, path: "a.goblin", size: 150, key_range: {1, 50})
      dt2 = fake_disk_table(level_key: 1, path: "b.goblin", size: 150, key_range: {51, 100})

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)

      assert {:compact, 2, _sources, _targets, _filter?, _compactor} = Compactor.next(compactor)
    end

    test "returns :noop when only target level has tables" do
      dt = fake_disk_table(level_key: 1, path: "a.goblin", size: 10)

      compactor =
        new_compactor()
        |> Compactor.put_into_level(dt)

      assert {:noop, _compactor} = Compactor.next(compactor)
    end

    test "removes source and target tables from levels" do
      compactor =
        Enum.reduce(1..4, new_compactor(), fn i, acc ->
          dt = fake_disk_table(level_key: 0, path: "#{i}.goblin", key_range: {i, i})
          Compactor.put_into_level(acc, dt)
        end)

      {:compact, _level, _sources, _targets, _filter?, compactor} = Compactor.next(compactor)

      assert Map.get(compactor.levels, 0, []) == []
    end
  end

  describe "compact/5" do
    test "merges level 0 sources into level 1", ctx do
      counter = :counters.new(1, [])
      ctx = Map.put(ctx, :counter, counter)

      dt1 = create_disk_table(ctx, 0, [{1, 0, "a"}, {3, 1, "c"}])
      dt2 = create_disk_table(ctx, 0, [{2, 2, "b"}, {4, 3, "d"}])

      compactor =
        new_compactor_with_disk_opts(ctx, flush_level_file_limit: 2)
        |> Compactor.put_into_level(dt1)
        |> Compactor.put_into_level(dt2)

      {:compact, target_level_key, sources, targets, filter?, compactor} =
        Compactor.next(compactor)

      assert {:ok, new_tables, old_tables} =
               Compactor.compact(compactor, target_level_key, sources, targets, filter?)

      assert length(new_tables) >= 1
      assert length(old_tables) == 2

      # Verify the new tables are level 1
      assert Enum.all?(new_tables, &(&1.level_key == 1))

      # Verify old tables match what was compacted
      old_files = Enum.map(old_tables, & &1.path) |> Enum.sort()
      expected_files = Enum.map([dt1, dt2], & &1.path) |> Enum.sort()
      assert old_files == expected_files

      # Read back the merged data to verify correctness
      [merged] = new_tables

      data =
        Goblin.Iterator.linear_stream(fn ->
          StreamIterator.new(merged)
        end)
        |> Enum.to_list()

      assert [{1, 0, "a"}, {2, 2, "b"}, {3, 1, "c"}, {4, 3, "d"}] == data
    end
  end
end
