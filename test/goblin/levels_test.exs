defmodule Goblin.LevelsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Goblin.Levels
  alias Goblin.DiskTable

  @opts [
    flush_level_file_limit: 4,
    level_base_size: 256,
    level_size_multiplier: 10
  ]

  describe "next/2" do
    test "returns nil if empty" do
      assert nil == Levels.next(%{}, @opts)
    end

    test "returns entire flush level when flush level exceeds flush level file limit" do
      dt1 = %DiskTable{id: "a", level_key: 0, key_range: {1, 2}}
      levels = Levels.put(%{}, dt1)
      assert nil == Levels.next(levels, @opts)

      dt2 = %DiskTable{id: "b", level_key: 0, key_range: {3, 4}}
      levels = Levels.put(levels, dt2)
      assert nil == Levels.next(levels, @opts)

      dt3 = %DiskTable{id: "c", level_key: 0, key_range: {5, 6}}
      levels = Levels.put(levels, dt3)
      assert nil == Levels.next(levels, @opts)

      dt4 = %DiskTable{id: "d", level_key: 0, key_range: {7, 8}}
      levels = Levels.put(levels, dt4)
      assert {:merge, 1, dts_to_merge, true, levels} = Levels.next(levels, @opts)
      assert Enum.sort_by([dt1, dt2, dt3, dt4], & &1.id) == Enum.sort_by(dts_to_merge, & &1.id)

      assert %{} == levels
    end

    test "returns filter_tombstones? = false when a deeper level exits" do
      dts = [
        %DiskTable{id: "a", level_key: 0, key_range: {1, 2}},
        %DiskTable{id: "b", level_key: 0, key_range: {3, 4}},
        %DiskTable{id: "c", level_key: 0, key_range: {5, 6}},
        %DiskTable{id: "d", level_key: 0, key_range: {7, 8}},
        %DiskTable{id: "e", level_key: 2, key_range: {9, 10}}
      ]

      levels = Enum.reduce(dts, %{}, &Levels.put(&2, &1))
      assert {:merge, 1, _, false, _} = Levels.next(levels, @opts)
    end

    test "returns a source and target(s) when level exceeds size limit" do
      dt1 = %DiskTable{id: "a", level_key: 1, key_range: {1, 2}, seq_range: {0, 1}, size: 100}
      dt2 = %DiskTable{id: "b", level_key: 1, key_range: {3, 4}, seq_range: {2, 3}, size: 256}

      levels = Levels.put(%{}, dt1)
      assert nil == Levels.next(levels, @opts)

      levels = Levels.put(levels, dt2)
      assert {:merge, 2, [^dt1], true, levels} = Levels.next(levels, @opts)
      assert %{1 => [dt2]} == levels
    end
  end

  @tag :property_tests
  property "merge returns all disk tables in flush level when exceeding flush level file limit" do
    check all(dts <- list_of(disk_table_generator(0), min_length: 2, max_length: 6)) do
      levels = Enum.reduce(dts, %{}, &Levels.put(&2, &1))

      if length(dts) >= @opts[:flush_level_file_limit] do
        assert {:merge, 1, dts_to_merge, true, _} = Levels.next(levels, @opts)
        assert Enum.sort_by(dts_to_merge, & &1.id) == Enum.sort_by(dts, & &1.id)
      else
        assert nil == Levels.next(levels, @opts)
      end
    end
  end

  @tag :property_tests
  property "merge returns single source disk table with overlapping key range target disk tables" do
    check all(
            lk <- positive_integer(),
            target_lk = lk + 1,
            source_dts <- list_of(disk_table_generator(lk), min_length: 4, max_length: 8),
            target_dts <- list_of(disk_table_generator(target_lk), min_length: 4, max_length: 8)
          ) do
      levels = Enum.reduce(source_dts ++ target_dts, %{}, &Levels.put(&2, &1))
      assert {:merge, ^target_lk, dts_to_merge, _, _} = Levels.next(levels, @opts)
      assert {[source], targets} = Enum.split_with(dts_to_merge, &(&1.level_key == lk))

      {source_min_key, source_max_key} = source.key_range

      Enum.each(targets, fn dt ->
        {min_key, max_key} = dt.key_range
        refute max_key < source_min_key
        refute min_key > source_max_key
      end)
    end
  end

  defp disk_table_generator(lk) do
    gen all(
          id <- string(:printable, min_length: 1),
          key1 <- term(),
          key2 <- term(),
          seq1 <- positive_integer(),
          seq2 <- positive_integer()
        ) do
      pot = if lk == 0, do: 0, else: lk - 1
      size = div(@opts[:level_base_size] * @opts[:level_size_multiplier] ** pot, 4)
      key_range = {min(key1, key2), max(key1, key2)}
      seq_range = {min(seq1, seq2), max(seq1, seq2)}

      %DiskTable{
        id: id,
        level_key: lk,
        key_range: key_range,
        seq_range: seq_range,
        size: size
      }
    end
  end
end
