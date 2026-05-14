defmodule Goblin.LevelsTest do
  use ExUnit.Case, async: true

  alias Goblin.Levels

  @opts [
    flush_level_file_limit: 4,
    level_base_size: 256,
    level_size_multiplier: 10
  ]

  defp fake(attrs) do
    Map.merge(
      %{id: "x.goblin", size: 0, key_range: {0, 100}, seq_range: {0, 0}},
      Map.new(attrs)
    )
  end

  describe "put/3" do
    test "adds a table to an empty level" do
      dt = fake(id: "a")
      assert Levels.put(%{}, 0, dt) == %{0 => [dt]}
    end

    test "prepends to an existing level so the latest insert is first" do
      a = fake(id: "a")
      b = fake(id: "b")

      levels =
        %{}
        |> Levels.put(0, a)
        |> Levels.put(0, b)

      assert levels[0] == [b, a]
    end

    test "puts at one level without touching others" do
      a = fake(id: "a")
      b = fake(id: "b")

      levels =
        %{}
        |> Levels.put(0, a)
        |> Levels.put(2, b)

      assert levels == %{0 => [a], 2 => [b]}
    end
  end

  describe "next/2 — no overflow" do
    test "empty levels returns nil" do
      assert Levels.next(%{}, @opts) == nil
    end

    test "level 0 below flush_level_file_limit returns nil" do
      levels = Enum.reduce(1..3, %{}, &Levels.put(&2, 0, fake(id: "a#{&1}")))
      assert Levels.next(levels, @opts) == nil
    end

    test "level 1 below the size threshold returns nil" do
      dt = fake(level_key: 1, size: 100, key_range: {0, 10})
      levels = Levels.put(%{}, 1, dt)
      assert Levels.next(levels, @opts) == nil
    end
  end

  describe "next/2 — level 0 overflow" do
    test "exactly flush_level_file_limit tables triggers a merge" do
      levels =
        Enum.reduce(1..4, %{}, fn i, acc ->
          Levels.put(acc, 0, fake(id: "a#{i}", key_range: {i, i}))
        end)

      assert {:merge, 1, sources, _filter?, levels_after} = Levels.next(levels, @opts)
      assert length(sources) == 4
      assert Map.has_key?(levels_after, 0) == false
    end

    test "filter_tombstones? is true when level 0 is the deepest populated level" do
      levels =
        Enum.reduce(1..4, %{}, fn i, acc ->
          Levels.put(acc, 0, fake(id: "a#{i}", key_range: {i, i}))
        end)

      assert {:merge, 1, _sources, true, _levels} = Levels.next(levels, @opts)
    end

    test "filter_tombstones? is false when a deeper level exists" do
      levels =
        Enum.reduce(1..4, %{}, fn i, acc ->
          Levels.put(acc, 0, fake(id: "a#{i}", key_range: {i, i}))
        end)
        |> Levels.put(5, fake(id: "deep", key_range: {1000, 1001}))

      assert {:merge, 1, _sources, false, _levels} = Levels.next(levels, @opts)
    end
  end

  describe "next/2 — level >= 1 overflow" do
    test "a single table sized at the threshold triggers a merge into the next level" do
      dt = fake(level_key: 1, size: 256, key_range: {0, 10}, seq_range: {0, 5})
      levels = Levels.put(%{}, 1, dt)

      assert {:merge, 2, [^dt], _filter?, _levels} = Levels.next(levels, @opts)
    end

    test "selects the oldest table (smallest seq_range[0]) as the source" do
      old = fake(id: "old", level_key: 1, size: 200, key_range: {0, 10}, seq_range: {0, 5})
      new = fake(id: "new", level_key: 1, size: 200, key_range: {20, 30}, seq_range: {10, 15})

      levels =
        %{}
        |> Levels.put(1, old)
        |> Levels.put(1, new)

      assert {:merge, 2, [^old], _filter?, levels_after} = Levels.next(levels, @opts)
      assert levels_after[1] == [new]
    end

    test "level 2's threshold scales by level_size_multiplier" do
      # Level 2 threshold: 256 * 10^1 = 2560 bytes. A 500-byte table is well under.
      dt = fake(level_key: 2, size: 500, key_range: {0, 10}, seq_range: {0, 5})
      levels = Levels.put(%{}, 2, dt)

      assert Levels.next(levels, @opts) == nil
    end
  end

  describe "next/2 — filter_tombstones?" do
    test "true when merging into the deepest level" do
      old = fake(id: "old", level_key: 1, size: 300, key_range: {0, 10}, seq_range: {0, 5})
      levels = Levels.put(%{}, 1, old)

      assert {:merge, 2, _sources, true, _levels} = Levels.next(levels, @opts)
    end

    test "false when merging into an intermediate level" do
      old = fake(id: "old", level_key: 1, size: 300, key_range: {0, 10}, seq_range: {0, 5})
      bottom = fake(id: "bottom", level_key: 5, size: 1, key_range: {500, 501}, seq_range: {0, 0})

      levels =
        %{}
        |> Levels.put(1, old)
        |> Levels.put(5, bottom)

      assert {:merge, 2, _sources, false, _levels} = Levels.next(levels, @opts)
    end
  end

  describe "next/2 — overlapping targets" do
    test "an overlapping target at target_lk is included in the merge and removed from levels" do
      src = fake(level_key: 1, size: 300, key_range: {1, 10}, seq_range: {10, 15})

      overlap =
        fake(id: "overlap", level_key: 2, size: 100, key_range: {5, 20}, seq_range: {0, 5})

      levels =
        %{}
        |> Levels.put(1, src)
        |> Levels.put(2, overlap)

      assert {:merge, 2, dts, _filter?, levels_after} = Levels.next(levels, @opts)

      assert src in dts
      assert overlap in dts
      refute Map.has_key?(levels_after, 2)
    end

    test "a non-overlapping target at target_lk is left in place" do
      src = fake(level_key: 1, size: 300, key_range: {1, 10}, seq_range: {10, 15})

      far_away =
        fake(id: "far_away", level_key: 2, size: 100, key_range: {100, 200}, seq_range: {0, 5})

      levels =
        %{}
        |> Levels.put(1, src)
        |> Levels.put(2, far_away)

      assert {:merge, 2, dts, _filter?, levels_after} = Levels.next(levels, @opts)

      assert dts == [src]
      assert levels_after[2] == [far_away]
    end

    test "bounding range spans multiple sources, picking up targets at either end" do
      a = fake(id: "a", level_key: 0, key_range: {0, 5}, seq_range: {0, 5})
      b = fake(id: "b", level_key: 0, key_range: {10, 15}, seq_range: {0, 5})
      c = fake(id: "c", level_key: 0, key_range: {20, 25}, seq_range: {0, 5})
      d = fake(id: "d", level_key: 0, key_range: {30, 35}, seq_range: {0, 5})

      hits_a =
        fake(id: "hits_a", level_key: 1, size: 1, key_range: {3, 12}, seq_range: {0, 0})

      hits_d =
        fake(id: "hits_d", level_key: 1, size: 1, key_range: {32, 40}, seq_range: {0, 0})

      out_of_range =
        fake(id: "out_of_range", level_key: 1, size: 1, key_range: {100, 200}, seq_range: {0, 0})

      levels =
        %{}
        |> Levels.put(0, a)
        |> Levels.put(0, b)
        |> Levels.put(0, c)
        |> Levels.put(0, d)
        |> Levels.put(1, hits_a)
        |> Levels.put(1, hits_d)
        |> Levels.put(1, out_of_range)

      assert {:merge, 1, dts, _filter?, levels_after} = Levels.next(levels, @opts)

      assert hits_a in dts
      assert hits_d in dts
      refute out_of_range in dts
      assert levels_after[1] == [out_of_range]
    end
  end

  describe "next/2 — level mutation" do
    test "after a level-0 overflow, level 0 is gone from the returned levels" do
      levels =
        Enum.reduce(1..4, %{}, fn i, acc ->
          Levels.put(acc, 0, fake(id: "a#{i}", key_range: {i, i}))
        end)

      {:merge, _, _, _, levels_after} = Levels.next(levels, @opts)

      refute Map.has_key?(levels_after, 0)
    end

    test "after a level-1 overflow, the source is removed but co-residents remain" do
      old = fake(id: "old", level_key: 1, size: 200, key_range: {0, 10}, seq_range: {0, 5})
      stays = fake(id: "stays", level_key: 1, size: 200, key_range: {20, 30}, seq_range: {10, 15})

      levels =
        %{}
        |> Levels.put(1, old)
        |> Levels.put(1, stays)

      {:merge, _, _, _, levels_after} = Levels.next(levels, @opts)

      assert levels_after[1] == [stays]
    end
  end
end
