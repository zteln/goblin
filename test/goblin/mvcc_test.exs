defmodule Goblin.MVCCTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Goblin.MVCC
  alias Goblin.MemTable
  alias Goblin.DiskTable

  setup do
    %{mvcc: MVCC.new()}
  end

  describe "snapshot isolation" do
    test "a pin sees the tables that were live when it was taken", ctx do
      [a, b, c] = [mem_table(), mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a, b], [])
      MVCC.put_version(ctx.mvcc, [c], [a])

      key = pin(ctx.mvcc)

      assert tables(ctx.mvcc, key) == MapSet.new([b, c])
    end

    test "tables added after a pin are invisible to it", ctx do
      [a, b] = [mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a], [])
      key = pin(ctx.mvcc)

      MVCC.put_version(ctx.mvcc, [b], [])

      assert tables(ctx.mvcc, key) == MapSet.new([a])
    end

    test "tables retired after a pin stay visible to it", ctx do
      [a, b] = [mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a], [])
      key = pin(ctx.mvcc)

      MVCC.put_version(ctx.mvcc, [b], [a])

      assert tables(ctx.mvcc, key) == MapSet.new([a])
    end

    test "pins taken at different times keep their own snapshots", ctx do
      [a, b] = [mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a], [])
      old = pin(ctx.mvcc)
      MVCC.put_version(ctx.mvcc, [b], [a])
      new = pin(ctx.mvcc)

      assert tables(ctx.mvcc, old) == MapSet.new([a])
      assert tables(ctx.mvcc, new) == MapSet.new([b])
    end
  end

  describe "pin/2 and unpin/2" do
    test "pin returns the latest sequence and highest level key", ctx do
      assert {0, -1} == MVCC.pin(ctx.mvcc, make_ref())

      disk = disk_table(2, {1, 10})
      MVCC.update_sequence(ctx.mvcc, 7)
      MVCC.put_version(ctx.mvcc, [disk], [])
      MVCC.put_version(ctx.mvcc, [mem_table()], [disk])

      assert {7, 2} == MVCC.pin(ctx.mvcc, make_ref())
    end

    test "reading through an inactive pin raises", ctx do
      key = make_ref()
      assert_raise ArgumentError, fn -> MVCC.get_all_tables(ctx.mvcc, key) end

      MVCC.pin(ctx.mvcc, key)
      MVCC.unpin(ctx.mvcc, key)
      assert_raise ArgumentError, fn -> MVCC.get_all_tables(ctx.mvcc, key) end
    end

    test "unpinning a pid releases only that process's pins", ctx do
      mine = make_ref()
      theirs = make_ref()
      MVCC.pin(ctx.mvcc, mine)
      Task.async(fn -> MVCC.pin(ctx.mvcc, theirs) end) |> Task.await()

      MVCC.unpin(ctx.mvcc, self())

      refute MVCC.pinned?(ctx.mvcc, mine)
      assert MVCC.pinned?(ctx.mvcc, theirs)
    end
  end

  describe "sweep/1" do
    test "keeps retired tables while a pin can see them", ctx do
      [a, b] = [mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a], [])
      key = pin(ctx.mvcc)
      MVCC.put_version(ctx.mvcc, [b], [a])

      assert [] == MVCC.sweep(ctx.mvcc)

      MVCC.unpin(ctx.mvcc, key)
      assert [a] == MVCC.sweep(ctx.mvcc)
    end

    test "reclaims tables retired before the oldest pin", ctx do
      [a, b] = [mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a], [])
      MVCC.put_version(ctx.mvcc, [b], [a])
      pin(ctx.mvcc)

      assert [a] == MVCC.sweep(ctx.mvcc)
    end

    test "returns each table at most once", ctx do
      [a, b] = [mem_table(), mem_table()]
      MVCC.put_version(ctx.mvcc, [a], [])
      MVCC.put_version(ctx.mvcc, [b], [a])

      assert [a] == MVCC.sweep(ctx.mvcc)
      assert [] == MVCC.sweep(ctx.mvcc)
    end
  end

  describe "get_matching_tables/4" do
    test "returns tables at the level whose key range contains a key", ctx do
      a = disk_table(1, {1, 10})
      b = disk_table(1, {20, 30})
      c = disk_table(1, {40, 50})
      MVCC.put_version(ctx.mvcc, [a, b, c, disk_table(2, {1, 50})], [])
      key = pin(ctx.mvcc)

      assert matching(ctx.mvcc, key, 1, [5, 45]) == MapSet.new([a, c])
      assert matching(ctx.mvcc, key, 1, [10, 20]) == MapSet.new([a, b])
      assert matching(ctx.mvcc, key, 1, [60]) == MapSet.new()
    end

    test "sees a retired table that shares its max key with its replacement", ctx do
      old = disk_table(1, {1, 10})
      MVCC.put_version(ctx.mvcc, [old], [])
      key = pin(ctx.mvcc)

      MVCC.put_version(ctx.mvcc, [disk_table(1, {5, 10})], [old])

      assert matching(ctx.mvcc, key, 1, [10]) == MapSet.new([old])
    end

    test "returns every table at levels -1 and 0 regardless of keys", ctx do
      [m1, m2] = [mem_table(), mem_table()]
      d1 = disk_table(0, {1, 10})
      d2 = disk_table(0, {20, 30})
      MVCC.put_version(ctx.mvcc, [m1, m2, d1, d2], [])
      key = pin(ctx.mvcc)

      assert matching(ctx.mvcc, key, -1, [100]) == MapSet.new([m1, m2])
      assert matching(ctx.mvcc, key, 0, [100]) == MapSet.new([d1, d2])
    end
  end

  @tag :property_tests
  property "pins keep their snapshot and sweep only reclaims unreachable tables" do
    check all(commands <- list_of(command(), max_length: 50)) do
      mvcc = MVCC.new()
      model = %{live: MapSet.new(), retired: MapSet.new(), pins: %{}, next: 0}

      model =
        Enum.reduce(commands, model, fn command, model ->
          model = run(mvcc, command, model)

          for {key, snapshot} <- model.pins do
            assert tables(mvcc, key) == snapshot
          end

          model
        end)

      for {key, _snapshot} <- model.pins, do: MVCC.unpin(mvcc, key)
      assert MapSet.new(MVCC.sweep(mvcc)) == model.retired
    end
  end

  @tag :property_tests
  property "get_matching_tables/4 returns the tables whose range contains a key" do
    check all(
            bounds <- uniq_list_of(integer(0..200), min_length: 2),
            keys <- list_of(integer(0..200), min_length: 1)
          ) do
      tables =
        bounds
        |> Enum.sort()
        |> Enum.chunk_every(2, 2, :discard)
        |> Enum.map(fn [min, max] -> disk_table(1, {min, max}) end)

      keys = keys |> Enum.sort() |> Enum.uniq()

      mvcc = MVCC.new()
      MVCC.put_version(mvcc, tables, [])
      key = pin(mvcc)

      expected =
        Enum.filter(tables, fn %{key_range: {min, max}} ->
          Enum.any?(keys, &(&1 in min..max))
        end)

      assert matching(mvcc, key, 1, keys) == MapSet.new(expected)
    end
  end

  defp run(mvcc, :add, model) do
    table = %MemTable{ref: model.next}
    MVCC.put_version(mvcc, [table], [])
    %{model | live: MapSet.put(model.live, table), next: model.next + 1}
  end

  defp run(mvcc, {:replace, i}, model) do
    case pick(model.live, i) do
      nil ->
        model

      old ->
        new = %MemTable{ref: model.next}
        MVCC.put_version(mvcc, [new], [old])

        %{
          model
          | live: model.live |> MapSet.delete(old) |> MapSet.put(new),
            retired: MapSet.put(model.retired, old),
            next: model.next + 1
        }
    end
  end

  defp run(mvcc, :pin, model) do
    key = pin(mvcc)
    %{model | pins: Map.put(model.pins, key, model.live)}
  end

  defp run(mvcc, {:unpin, i}, model) do
    case pick(Map.keys(model.pins), i) do
      nil ->
        model

      key ->
        MVCC.unpin(mvcc, key)
        %{model | pins: Map.delete(model.pins, key)}
    end
  end

  defp run(mvcc, :sweep, model) do
    swept = MapSet.new(MVCC.sweep(mvcc))
    reachable = model.pins |> Map.values() |> Enum.reduce(model.live, &MapSet.union/2)

    assert MapSet.subset?(swept, model.retired)
    assert MapSet.disjoint?(swept, reachable)

    %{model | retired: MapSet.difference(model.retired, swept)}
  end

  defp command do
    one_of([
      constant(:add),
      tuple({constant(:replace), non_negative_integer()}),
      constant(:pin),
      tuple({constant(:unpin), non_negative_integer()}),
      constant(:sweep)
    ])
  end

  defp pick(enumerable, i) do
    case Enum.count(enumerable) do
      0 -> nil
      count -> Enum.at(enumerable, rem(i, count))
    end
  end

  defp pin(mvcc) do
    key = make_ref()
    MVCC.pin(mvcc, key)
    key
  end

  defp tables(mvcc, key), do: MapSet.new(MVCC.get_all_tables(mvcc, key))

  defp matching(mvcc, key, level_key, keys),
    do: MapSet.new(MVCC.get_matching_tables(mvcc, key, level_key, keys))

  defp mem_table, do: %MemTable{ref: make_ref()}

  defp disk_table(level_key, key_range),
    do: %DiskTable{id: make_ref(), level_key: level_key, key_range: key_range}
end
