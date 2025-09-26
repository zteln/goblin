defmodule SeaGoat.Store.LevelsTest do
  use ExUnit.Case, async: true
  alias SeaGoat.Store.Levels

  test "insert/3 inserts and updates entry" do
    assert %{level: [:entry1]} = levels = Levels.insert(Levels.new(), :level, :entry1)
    assert %{level: [:entry2, :entry1]} == Levels.insert(levels, :level, :entry2)
  end

  test "remove/3 removes entries in a level" do
    levels =
      Levels.new()
      |> Levels.insert(:level, 1)
      |> Levels.insert(:level, 2)
      |> Levels.insert(:level, 3)
      |> Levels.insert(:level, 4)
      |> Levels.insert(:level, 5)

    assert %{level: [5, 3, 1]} == Levels.remove(levels, :level, &(rem(&1, 2) == 0))
  end

  test "levels/1 outputs the level keys sorted" do
    levels =
      Levels.new()
      |> Levels.insert(1, 1)
      |> Levels.insert(2, 2)
      |> Levels.insert(3, 3)
      |> Levels.insert(4, 4)
      |> Levels.insert(5, 5)

    assert [1, 2, 3, 4, 5] == Levels.levels(levels)
  end

  test "get_all_entries/4 returns all entries in a level matching a criteria" do
    levels =
      Levels.new()
      |> Levels.insert(:level, 1)
      |> Levels.insert(:level, 2)
      |> Levels.insert(:level, 3)
      |> Levels.insert(:level, 4)
      |> Levels.insert(:level, 5)

    assert [5, 4, 3, 2] == Levels.get_all_entries(levels, :level, &(&1 > 1))
    assert [6, 5, 4, 3] == Levels.get_all_entries(levels, :level, &(&1 > 1), &(&1 + 1))
  end
end
