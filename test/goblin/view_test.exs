defmodule Goblin.ViewTest do
  use ExUnit.Case, async: true

  alias Goblin.View

  setup do
    %{view: View.new()}
  end

  defp fake_table(id), do: %{id: id}

  defp snapshot_versions(view) do
    view
    |> :ets.match({{:snapshot, :"$1"}, :_, :_, :_, :_})
    |> List.flatten()
    |> Enum.sort()
  end

  describe "new/0" do
    test "fresh view: add_reader raises before any snapshot exists", ctx do
      assert_raise RuntimeError, ~r/reader before any snapshots/, fn ->
        View.add_reader(ctx.view, make_ref())
      end
    end

    test "fresh view: get_tables returns [] for any version / filter", ctx do
      assert View.get_tables(ctx.view, 0) == []
      assert View.get_tables(ctx.view, 0, -1) == []
      assert View.get_tables(ctx.view, 0, 5) == []
      assert View.get_tables(ctx.view, 7) == []
    end
  end

  describe "put_sequence/2" do
    test "raises if called before any snapshot exists", ctx do
      assert_raise RuntimeError, ~r/no existing snapshots/, fn ->
        View.put_sequence(ctx.view, 42)
      end
    end

    test "successive calls update the latest snapshot's seq in place", ctx do
      :ok = View.put_snapshot(ctx.view, [], %{})
      :ok = View.put_sequence(ctx.view, 5)
      :ok = View.put_sequence(ctx.view, 15)

      assert {15, -1, 1} = View.add_reader(ctx.view, make_ref())
      assert snapshot_versions(ctx.view) == [1]
    end

    test "after put_snapshot, put_sequence updates the latest snapshot's seq", ctx do
      :ok = View.put_snapshot(ctx.view, [], %{0 => [fake_table(:d)]})
      :ok = View.put_sequence(ctx.view, 99)

      assert {99, 0, 1} = View.add_reader(ctx.view, make_ref())
    end
  end

  describe "put_snapshot/3" do
    test "first call on empty view creates v=1, not v=0", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m)], %{})

      assert {_, _, 1} = View.add_reader(ctx.view, make_ref())
      assert snapshot_versions(ctx.view) == [1]
    end

    test "max_lk is derived from the levels keys (empty → -1)", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m)], %{})
      assert {_, -1, _} = View.add_reader(ctx.view, make_ref())
    end

    test "max_lk reflects the largest level key", _ctx do
      view = View.new()
      :ok = View.put_snapshot(view, [], %{0 => [fake_table(:a)], 3 => [fake_table(:c)]})

      assert {_, 3, _} = View.add_reader(view, make_ref())
    end

    test "copies the current seq from the latest snapshot onto the new one", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m1)], %{})
      :ok = View.put_sequence(ctx.view, 7)
      :ok = View.put_snapshot(ctx.view, [fake_table(:m2)], %{})

      assert {7, -1, 2} = View.add_reader(ctx.view, make_ref())
    end

    test "each call increments version", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m1)], %{})
      assert {_, _, 1} = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [fake_table(:m2)], %{})
      assert {_, _, 2} = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [fake_table(:m3)], %{})
      assert {_, _, 3} = View.add_reader(ctx.view, make_ref())
    end
  end

  describe "get_tables/2" do
    test "mem_tables come first, then level tables", ctx do
      m1 = fake_table(:m1)
      d0a = fake_table(:d0a)
      d0b = fake_table(:d0b)
      :ok = View.put_snapshot(ctx.view, [m1], %{0 => [d0a, d0b]})

      assert View.get_tables(ctx.view, 1) == [m1, d0a, d0b]
    end

    test "includes tables from all levels", ctx do
      m1 = fake_table(:m1)
      d0 = fake_table(:d0)
      d1 = fake_table(:d1)
      :ok = View.put_snapshot(ctx.view, [m1], %{0 => [d0], 1 => [d1]})

      tables = View.get_tables(ctx.view, 1)

      assert hd(tables) == m1
      assert MapSet.new(tables) == MapSet.new([m1, d0, d1])
    end

    test "unknown version returns []", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m)], %{})

      assert View.get_tables(ctx.view, 999) == []
    end

    test "old version remains accessible until sweep deletes it", ctx do
      m1 = fake_table(:m1)
      m2 = fake_table(:m2)
      :ok = View.put_snapshot(ctx.view, [m1], %{})
      :ok = View.put_snapshot(ctx.view, [m2], %{})

      assert View.get_tables(ctx.view, 1) == [m1]
      assert View.get_tables(ctx.view, 2) == [m2]
    end
  end

  describe "get_tables/3 with lk = -1" do
    test "returns mem_tables only, ignoring levels", ctx do
      m1 = fake_table(:m1)
      :ok = View.put_snapshot(ctx.view, [m1], %{0 => [fake_table(:d)]})

      assert View.get_tables(ctx.view, 1, -1) == [m1]
    end

    test "empty mem_tables returns []", ctx do
      :ok = View.put_snapshot(ctx.view, [], %{0 => [fake_table(:d)]})

      assert View.get_tables(ctx.view, 1, -1) == []
    end
  end

  describe "get_tables/3 with positive lk" do
    test "returns only that level's tables", ctx do
      d0 = fake_table(:d0)
      d1 = fake_table(:d1)
      :ok = View.put_snapshot(ctx.view, [], %{0 => [d0], 1 => [d1]})

      assert View.get_tables(ctx.view, 1, 0) == [d0]
      assert View.get_tables(ctx.view, 1, 1) == [d1]
    end

    test "returns [] when the levels map has no entry for lk", ctx do
      :ok = View.put_snapshot(ctx.view, [], %{0 => [fake_table(:d)]})

      assert View.get_tables(ctx.view, 1, 99) == []
    end

    test "returns [] when no snapshot exists at the given version", ctx do
      :ok = View.put_snapshot(ctx.view, [], %{0 => [fake_table(:d)]})

      assert View.get_tables(ctx.view, 7, 0) == []
    end
  end

  describe "add_reader/2 + release_reader/2" do
    test "pins to the current version even as new snapshots are installed", ctx do
      m1 = fake_table(:m1)
      m2 = fake_table(:m2)
      :ok = View.put_snapshot(ctx.view, [m1], %{})

      {_, _, v1} = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [m2], %{})

      assert v1 == 1
      assert View.get_tables(ctx.view, v1) == [m1]
    end

    test "release_reader on a never-acquired key is a no-op", ctx do
      assert :ok = View.release_reader(ctx.view, make_ref())
    end

    test "release_reader removes pending and version-bound rows for the key", ctx do
      :ok = View.put_snapshot(ctx.view, [], %{})
      key = make_ref()

      # simulate the in-flight state where both reader rows momentarily co-exist
      :ets.insert(ctx.view, {{:reader, :pending, key}})
      :ets.insert(ctx.view, {{:reader, 1, key}})

      :ok = View.release_reader(ctx.view, key)

      assert :ets.match(ctx.view, {{:reader, :_, key}}) == []
    end

    test "release of one reader does not affect another", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m1)], %{})

      key_a = make_ref()
      key_b = make_ref()
      _ = View.add_reader(ctx.view, key_a)

      :ok = View.put_snapshot(ctx.view, [fake_table(:m2)], %{})

      _ = View.add_reader(ctx.view, key_b)

      :ok = View.release_reader(ctx.view, key_a)

      assert :ets.match(ctx.view, {{:reader, :_, key_a}}) == []
      assert :ets.match(ctx.view, {{:reader, :_, key_b}}) != []
    end
  end

  describe "sweep/1 — basic" do
    test "empty view returns [] and does not crash", ctx do
      assert View.sweep(ctx.view) == []
    end

    test "single snapshot, no readers: the current snapshot is always protected", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:t)], %{})

      assert View.sweep(ctx.view) == []
    end

    test "two snapshots, disjoint tables: older's tables returned, row deleted", ctx do
      t_old = fake_table(:t_old)
      t_new = fake_table(:t_new)
      :ok = View.put_snapshot(ctx.view, [t_old], %{})
      :ok = View.put_snapshot(ctx.view, [t_new], %{})

      assert View.sweep(ctx.view) == [t_old]
      assert snapshot_versions(ctx.view) == [2]
    end

    test "three snapshots, no readers: union of old-only tables returned", ctx do
      a = fake_table(:a)
      b = fake_table(:b)
      c = fake_table(:c)
      :ok = View.put_snapshot(ctx.view, [a], %{})
      :ok = View.put_snapshot(ctx.view, [b], %{})
      :ok = View.put_snapshot(ctx.view, [c], %{})

      assert Enum.sort_by(View.sweep(ctx.view), & &1.id) == [a, b]
      assert snapshot_versions(ctx.view) == [3]
    end

    test "a table shared between an old and the current snapshot is NOT swept", ctx do
      shared = fake_table(:shared)
      m2 = fake_table(:m2)
      :ok = View.put_snapshot(ctx.view, [shared], %{})
      :ok = View.put_snapshot(ctx.view, [shared, m2], %{})

      assert View.sweep(ctx.view) == []
    end
  end

  describe "sweep/1 — reader protection" do
    test "reader pinned at v=1 protects across a later put_snapshot", ctx do
      m1 = fake_table(:m1)
      m2 = fake_table(:m2)
      :ok = View.put_snapshot(ctx.view, [m1], %{})
      _ = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [m2], %{})

      assert View.sweep(ctx.view) == []
      # snapshot row at v=1 must still exist while the reader holds it
      assert snapshot_versions(ctx.view) == [1, 2]
    end

    test "after release_reader, previously-protected tables become sweepable", ctx do
      m1 = fake_table(:m1)
      m2 = fake_table(:m2)
      key = make_ref()
      :ok = View.put_snapshot(ctx.view, [m1], %{})
      _ = View.add_reader(ctx.view, key)

      :ok = View.put_snapshot(ctx.view, [m2], %{})

      assert View.sweep(ctx.view) == []

      :ok = View.release_reader(ctx.view, key)

      assert View.sweep(ctx.view) == [m1]
    end

    test "reader at v=1, snapshots at v=1..3: only v=2-only tables are swept", ctx do
      a = fake_table(:a)
      b = fake_table(:b)
      c = fake_table(:c)
      :ok = View.put_snapshot(ctx.view, [a], %{})
      _ = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [b], %{})
      :ok = View.put_snapshot(ctx.view, [c], %{})

      assert View.sweep(ctx.view) == [b]
      # v=1 is still pinned, v=2 is dropped, v=3 is current
      assert snapshot_versions(ctx.view) == [1, 3]
    end

    test "two readers at different versions each protect their own snapshot", ctx do
      a = fake_table(:a)
      b = fake_table(:b)
      c = fake_table(:c)

      :ok = View.put_snapshot(ctx.view, [a], %{})
      _ = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [b], %{})
      _ = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [c], %{})

      assert View.sweep(ctx.view) == []
      assert snapshot_versions(ctx.view) == [1, 2, 3]
    end
  end

  describe "sweep/1 — pending-reader window" do
    # add_reader inserts {:reader, :pending, key} before reading current_meta, then inserts
    # {:reader, version, key}, then deletes the pending row. During that window sweep cannot
    # know which version the in-flight reader will pin, so it must bail. These tests directly
    # manipulate the {:reader, :pending, _} row to exercise that branch deterministically.
    # If View's ETS schema changes, update these tests.
    test "a pending row forces sweep to return [] even when work is sweepable", ctx do
      t_old = fake_table(:t_old)
      t_new = fake_table(:t_new)
      :ok = View.put_snapshot(ctx.view, [t_old], %{})
      :ok = View.put_snapshot(ctx.view, [t_new], %{})

      :ets.insert(ctx.view, {{:reader, :pending, make_ref()}})

      assert View.sweep(ctx.view) == []
      # nothing was deleted while pending was set
      assert snapshot_versions(ctx.view) == [1, 2]
    end

    test "after the pending row is cleared, sweep proceeds", ctx do
      t_old = fake_table(:t_old)
      t_new = fake_table(:t_new)
      :ok = View.put_snapshot(ctx.view, [t_old], %{})
      :ok = View.put_snapshot(ctx.view, [t_new], %{})

      pending_key = make_ref()
      :ets.insert(ctx.view, {{:reader, :pending, pending_key}})
      assert View.sweep(ctx.view) == []

      :ets.delete(ctx.view, {:reader, :pending, pending_key})

      assert View.sweep(ctx.view) == [t_old]
    end
  end

  describe "sweep/1 — concurrent reader spans a snapshot install" do
    test "a reader held across put_snapshot protects its snapshot until release", ctx do
      m1 = fake_table(:m1)
      m2 = fake_table(:m2)
      :ok = View.put_snapshot(ctx.view, [m1], %{})

      test_pid = self()

      task =
        Task.async(fn ->
          key = make_ref()
          {_, _, v} = View.add_reader(ctx.view, key)
          send(test_pid, {:acked, v})

          receive do
            :go -> :ok
          end

          :ok = View.release_reader(ctx.view, key)
        end)

      assert_receive {:acked, 1}, 1_000

      :ok = View.put_snapshot(ctx.view, [m2], %{})

      assert View.sweep(ctx.view) == []

      send(task.pid, :go)
      :ok = Task.await(task)

      assert View.sweep(ctx.view) == [m1]
    end
  end

  describe "add_reader/2 — invariant: snapshot must exist" do
    test "raises if called before any snapshot is installed", ctx do
      assert_raise RuntimeError, ~r/reader before any snapshots/, fn ->
        View.add_reader(ctx.view, make_ref())
      end
    end

    test "the failed call leaves no pending row behind", ctx do
      key = make_ref()

      assert_raise RuntimeError, fn -> View.add_reader(ctx.view, key) end

      assert :ets.match(ctx.view, {{:reader, :_, key}}) == []
    end

    test "succeeds once a snapshot is installed", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:m1)], %{})

      assert {_, _, 1} = View.add_reader(ctx.view, make_ref())
    end
  end

  describe "properties" do
    test "sweep is idempotent when nothing changes in between", ctx do
      :ok = View.put_snapshot(ctx.view, [fake_table(:a)], %{})
      :ok = View.put_snapshot(ctx.view, [fake_table(:b)], %{})

      _ = View.sweep(ctx.view)
      assert View.sweep(ctx.view) == []
    end

    test "sweep return is disjoint from get_tables of every live snapshot", ctx do
      a = fake_table(:a)
      b = fake_table(:b)
      c = fake_table(:c)
      shared = fake_table(:shared)

      :ok = View.put_snapshot(ctx.view, [a, shared], %{})
      _ = View.add_reader(ctx.view, make_ref())

      :ok = View.put_snapshot(ctx.view, [b], %{})
      :ok = View.put_snapshot(ctx.view, [c, shared], %{})

      swept = MapSet.new(View.sweep(ctx.view))

      for v <- snapshot_versions(ctx.view) do
        live = MapSet.new(View.get_tables(ctx.view, v))
        assert MapSet.disjoint?(swept, live), "swept overlaps with snapshot v=#{v}"
      end
    end
  end
end
