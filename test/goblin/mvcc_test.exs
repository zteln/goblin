defmodule Goblin.MVCCTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Goblin.MVCC

  setup do
    %{mvcc: MVCC.new()}
  end

  describe "put_snapshot/4" do
    test "increments version for every snapshot", ctx do
      assert :ok == MVCC.put_snapshot(ctx.mvcc, [], %{}, 0)
      assert {_, _, 0} = MVCC.add_reader(ctx.mvcc, make_ref())

      assert :ok == MVCC.put_snapshot(ctx.mvcc, [], %{}, 0)
      assert {_, _, 1} = MVCC.add_reader(ctx.mvcc, make_ref())

      assert :ok == MVCC.put_snapshot(ctx.mvcc, [], %{}, 0)
      assert {_, _, 2} = MVCC.add_reader(ctx.mvcc, make_ref())
    end

    test "maximum level key defaults to -1", ctx do
      assert :ok == MVCC.put_snapshot(ctx.mvcc, [], %{}, 0)
      assert {_, -1, _} = MVCC.add_reader(ctx.mvcc, make_ref())
    end

    test "maximum level key is derived from provided levels", ctx do
      assert :ok == MVCC.put_snapshot(ctx.mvcc, [], %{5 => %{}}, 0)
      assert {_, 5, _} = MVCC.add_reader(ctx.mvcc, make_ref())
    end
  end

  describe "get_tables/2/3" do
    test "can get all tables in snapshot", ctx do
      MVCC.put_snapshot(
        ctx.mvcc,
        [%{id: :mem1}, %{id: :mem2}],
        %{0 => [%{id: :disk0}], 1 => [%{id: :disk1}]},
        0
      )

      assert MapSet.new([
               %{id: :mem1},
               %{id: :mem2},
               %{id: :disk0},
               %{id: :disk1}
             ]) == MVCC.get_tables(ctx.mvcc, 0) |> MapSet.new()
    end

    test "only gets tables for provided version", ctx do
      MVCC.put_snapshot(
        ctx.mvcc,
        [%{id: :mem1}, %{id: :mem2}],
        %{0 => [%{id: :disk0}], 1 => [%{id: :disk1}]},
        0
      )

      MVCC.put_snapshot(
        ctx.mvcc,
        [%{id: :mem2}, %{id: :mem3}],
        %{1 => [%{id: :disk1}]},
        0
      )

      assert MapSet.new([
               %{id: :mem1},
               %{id: :mem2},
               %{id: :disk0},
               %{id: :disk1}
             ]) == MVCC.get_tables(ctx.mvcc, 0) |> MapSet.new()

      assert MapSet.new([
               %{id: :mem2},
               %{id: :mem3},
               %{id: :disk1}
             ]) == MVCC.get_tables(ctx.mvcc, 1) |> MapSet.new()
    end

    test "returns empty for unknown version", ctx do
      assert [] == MVCC.get_tables(ctx.mvcc, 99)
    end

    test "can get tables corresponding to provided level key", ctx do
      MVCC.put_snapshot(
        ctx.mvcc,
        [%{id: :mem1}, %{id: :mem2}],
        %{0 => [%{id: :disk0}], 1 => [%{id: :disk1}]},
        0
      )

      assert MapSet.new([%{id: :mem1}, %{id: :mem2}]) ==
               MVCC.get_tables(ctx.mvcc, 0, -1) |> MapSet.new()

      assert [%{id: :disk0}] == MVCC.get_tables(ctx.mvcc, 0, 0)
      assert [%{id: :disk1}] == MVCC.get_tables(ctx.mvcc, 0, 1)
      assert [] == MVCC.get_tables(ctx.mvcc, 0, 2)
    end
  end

  describe "add_reader/2, release_reader/2" do
    test "pins to current snapshot", ctx do
      reader_key = make_ref()
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)
      assert {0, -1, v1} = MVCC.add_reader(ctx.mvcc, reader_key)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem2}], %{}, 1)
      assert v1 == 0
      assert [%{id: :mem1}] == MVCC.get_tables(ctx.mvcc, v1)
    end

    test "can release non-existing reader", ctx do
      assert :ok == MVCC.release_reader(ctx.mvcc, make_ref())
    end

    test "release of one reader does not affect another reader", ctx do
      key1 = make_ref()
      key2 = make_ref()
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)

      MVCC.add_reader(ctx.mvcc, key1)
      MVCC.add_reader(ctx.mvcc, key2)

      assert :ok == MVCC.release_reader(ctx.mvcc, key1)

      assert :ets.match(ctx.mvcc, {{:reader, :_, key1}}) == []
      assert :ets.match(ctx.mvcc, {{:reader, :_, key2}}) != []
    end

    test "cannot add_reader before snapshot exists", ctx do
      assert_raise RuntimeError, fn ->
        MVCC.add_reader(ctx.mvcc, make_ref())
      end
    end

    test "pending readers are cleaned up if the reader fails to be added", ctx do
      key = make_ref()
      assert_raise RuntimeError, fn -> MVCC.add_reader(ctx.mvcc, key) end
      assert [] == :ets.match(ctx.mvcc, {{:reader, :_, key}})
    end
  end

  describe "sweep/1" do
    test "returns [] for empty MVCC table", ctx do
      assert [] == MVCC.sweep(ctx.mvcc)
    end

    test "returns [] for single snapshot", ctx do
      MVCC.put_snapshot(ctx.mvcc, [], %{}, 0)
      assert [] == MVCC.sweep(ctx.mvcc)
    end

    test "returns older table in disjoint snapshots", ctx do
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem2}], %{}, 0)
      assert [%{id: :mem1}] == MVCC.sweep(ctx.mvcc)
    end

    test "returns union of retired tables", ctx do
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem2}, %{id: :mem3}], %{}, 0)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem3}, %{id: :mem4}], %{}, 0)
      assert MapSet.new([%{id: :mem1}, %{id: :mem2}]) == MVCC.sweep(ctx.mvcc) |> MapSet.new()
    end

    test "does not return table pinned by reader", ctx do
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem2}, %{id: :mem3}], %{}, 0)
      MVCC.add_reader(ctx.mvcc, make_ref())
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem3}, %{id: :mem4}], %{}, 0)
      assert [%{id: :mem1}] == MVCC.sweep(ctx.mvcc)
    end

    test "returns previously pinned tables", ctx do
      key = make_ref()
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)
      MVCC.add_reader(ctx.mvcc, key)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem2}], %{}, 0)
      assert [] == MVCC.sweep(ctx.mvcc)
      MVCC.release_reader(ctx.mvcc, key)
      assert [%{id: :mem1}] == MVCC.sweep(ctx.mvcc)
    end

    test "does not sweep if a pending reader exists", ctx do
      :ets.insert(ctx.mvcc, {{:reader, :pending, make_ref()}})
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem1}], %{}, 0)
      MVCC.put_snapshot(ctx.mvcc, [%{id: :mem2}], %{}, 0)
      assert [] == MVCC.sweep(ctx.mvcc)
    end
  end

  @tag :property_tests
  property "every installed version remains independently readable (version isolation)" do
    check all(
            pool <- pool_gen(),
            snapshots <- list_of(subset_gen(length(pool)), min_length: 1, max_length: 8)
          ) do
      mvcc = MVCC.new()

      expected =
        snapshots
        |> Enum.with_index()
        |> Enum.map(fn {idxs, v} ->
          tables = Enum.map(idxs, &Enum.at(pool, &1))
          {mts, dts} = Enum.split_with(tables, &match?(%Goblin.MemTable{}, &1))
          levels = Enum.group_by(dts, & &1.level_key)
          :ok = MVCC.put_snapshot(mvcc, mts, levels, v)
          {v, MapSet.new(tables)}
        end)

      for {v, want} <- expected do
        assert MapSet.new(MVCC.get_tables(mvcc, v)) == want
      end
    end
  end

  @tag :property_tests
  property "sweep never reclaims any live or pinned snapshots" do
    check all(
            pool <- pool_gen(),
            cmds <- list_of(command_gen(length(pool)), max_length: 30)
          ) do
      mvcc = MVCC.new()
      state = %{versions: %{}, current: -1, readers: %{}, seq: 0}

      Enum.reduce(cmds, state, fn cmd, acc -> step(mvcc, pool, cmd, acc) end)
    end
  end

  defp step(mvcc, pool, {:snapshot, idxs}, state) do
    tables = Enum.map(idxs, &Enum.at(pool, &1))
    {mts, dts} = Enum.split_with(tables, &match?(%Goblin.MemTable{}, &1))
    levels = Enum.group_by(dts, & &1.level_key)
    v = state.current + 1
    :ok = MVCC.put_snapshot(mvcc, mts, levels, state.seq + 1)
    assert MapSet.new(MVCC.get_tables(mvcc, v)) == MapSet.new(tables)

    %{
      state
      | current: v,
        seq: state.seq + 1,
        versions: Map.put(state.versions, v, MapSet.new(tables))
    }
  end

  defp step(_mvcc, _pool, :add_reader, %{current: -1} = state), do: state

  defp step(mvcc, _pool, :add_reader, state) do
    key = make_ref()
    {_, _, v} = MVCC.add_reader(mvcc, key)
    assert v == state.current
    %{state | readers: Map.put(state.readers, key, v)}
  end

  defp step(_mvcc, _pool, :release_reader, %{readers: readers} = state)
       when map_size(readers) == 0,
       do: state

  defp step(mvcc, _pool, :release_reader, state) do
    {key, _} = Enum.min_by(state.readers, fn {_k, v} -> v end)
    :ok = MVCC.release_reader(mvcc, key)
    %{state | readers: Map.delete(state.readers, key)}
  end

  defp step(mvcc, _pool, :sweep, %{current: -1} = state) do
    assert MVCC.sweep(mvcc) == []
    state
  end

  defp step(mvcc, _pool, :sweep, state) do
    swept = MapSet.new(MVCC.sweep(mvcc))
    protected_versions = MapSet.new([state.current | Map.values(state.readers)])

    for v <- protected_versions do
      live = MapSet.new(MVCC.get_tables(mvcc, v))
      assert MapSet.disjoint?(swept, live)
      assert live == state.versions[v]
    end

    assert MVCC.sweep(mvcc) == []
    state
  end

  defp command_gen(n) do
    one_of([
      tuple({constant(:snapshot), subset_gen(n)}),
      constant(:add_reader),
      constant(:release_reader),
      constant(:sweep)
    ])
  end

  defp pool_gen do
    gen all(kinds <- list_of(member_of([:mem, :disk]), min_length: 1, max_length: 8)) do
      kinds
      |> Enum.with_index()
      |> Enum.map(fn
        {:mem, i} -> %Goblin.MemTable{id: "t#{i}"}
        {:disk, i} -> %Goblin.DiskTable{id: "t#{i}", level_key: rem(i, 3)}
      end)
    end
  end

  defp subset_gen(pool_size) do
    gen all(idxs <- list_of(integer(0..(pool_size - 1)), max_length: pool_size)) do
      Enum.uniq(idxs)
    end
  end
end
