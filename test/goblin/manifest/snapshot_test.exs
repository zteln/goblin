defmodule Goblin.Manifest.SnapshotTest do
  use ExUnit.Case, async: true
  alias Goblin.Manifest.Snapshot

  describe "order/1" do
    test "returns 0 for a fresh snapshot" do
      assert 0 == Snapshot.order(%Snapshot{})
    end

    test "returns the order from the last applied update" do
      snapshot = Snapshot.update(%Snapshot{}, {5, :sequence, 1})
      assert 5 == Snapshot.order(snapshot)

      snapshot = Snapshot.update(snapshot, {10, :wal, "w.goblin"})
      assert 10 == Snapshot.order(snapshot)
    end
  end

  describe "update/2" do
    test "adding disk tables increases count" do
      snapshot = %Snapshot{}
      assert %{disk_table_count: 0} = snapshot
      assert %{disk_table_count: 1} = Snapshot.update(snapshot, {1, :disk_table_added, "foo"})
    end

    test "removing disk tables adds them to dirt" do
      snapshot = %Snapshot{}
      assert %{dirt: []} = snapshot

      snapshot = Snapshot.update(snapshot, {1, :disk_table_added, "foo"})
      assert %{dirt: ["foo"]} = Snapshot.update(snapshot, {2, :disk_table_removed, "foo"})
    end

    test "setting sequence updates the sequence field" do
      snapshot = %Snapshot{}
      assert %{sequence: 0} = snapshot

      assert %{sequence: 42} = Snapshot.update(snapshot, {1, :sequence, 42})
    end

    test "setting wal updates the wal field" do
      snapshot = %Snapshot{}
      assert %{wal: nil} = snapshot

      assert %{wal: "wal_0.goblin"} =
               Snapshot.update(snapshot, {1, :wal, "wal_0.goblin"})
    end

    test "adding a new wal moves the current wal to wals" do
      snapshot = %Snapshot{}
      assert %{wals: []} = snapshot

      snapshot = Snapshot.update(snapshot, {1, :wal, "wal_0.goblin"})
      assert %{wal: "wal_0.goblin", wals: []} = snapshot

      snapshot = Snapshot.update(snapshot, {2, :wal, "wal_1.goblin"})
      assert %{wal: "wal_1.goblin", wals: ["wal_0.goblin"]} = snapshot
    end

    test "wal_removed removes from wals list" do
      snapshot =
        %Snapshot{}
        |> Snapshot.update({1, :wal, "wal_0.goblin"})
        |> Snapshot.update({2, :wal, "wal_1.goblin"})

      assert %{wals: ["wal_0.goblin"]} = snapshot

      snapshot = Snapshot.update(snapshot, {3, :wal_removed, "wal_0.goblin"})
      assert %{wals: []} = snapshot
    end

    test "applying a list of updates processes all" do
      snapshot =
        Snapshot.update(%Snapshot{}, [
          {1, :sequence, 10},
          {2, :wal, "wal.goblin"},
          {3, :disk_table_added, "sst_0.goblin"}
        ])

      assert %{sequence: 10, wal: "wal.goblin", disk_tables: ["sst_0.goblin"], disk_table_count: 1} =
               snapshot
    end

    test "snapshot term replaces the entire snapshot" do
      original = Snapshot.update(%Snapshot{}, {1, :sequence, 99})

      replacement = %Snapshot{sequence: 5, wal: "other.goblin"}
      result = Snapshot.update(original, {:snapshot, replacement})

      assert result == replacement
    end

    test "ignores update with order equal to current order" do
      snapshot = Snapshot.update(%Snapshot{}, {5, :sequence, 1})

      snapshot = Snapshot.update(snapshot, {5, :sequence, 99})
      assert %{sequence: 1, order: 5} = snapshot
    end

    test "ignores update with order less than current order" do
      snapshot = Snapshot.update(%Snapshot{}, {5, :sequence, 1})

      snapshot = Snapshot.update(snapshot, {3, :sequence, 99})
      assert %{sequence: 1, order: 5} = snapshot
    end

    test "updates order field on each applied update" do
      snapshot =
        %Snapshot{}
        |> Snapshot.update({1, :disk_table_added, "foo"})
        |> Snapshot.update({2, :disk_table_removed, "foo"})

      assert %{order: 2} = snapshot
    end
  end

  describe "clear_dirt/1" do
    test "clears the dirt list" do
      snapshot =
        %Snapshot{}
        |> Snapshot.update({1, :disk_table_added, "foo"})
        |> Snapshot.update({2, :disk_table_removed, "foo"})

      assert %{dirt: ["foo"]} = snapshot

      snapshot = Snapshot.clear_dirt(snapshot)
      assert %{dirt: []} = snapshot
    end

    test "preserves other fields when clearing dirt" do
      snapshot =
        %Snapshot{}
        |> Snapshot.update({1, :sequence, 42})
        |> Snapshot.update({2, :wal, "wal.goblin"})
        |> Snapshot.update({3, :disk_table_added, "foo"})
        |> Snapshot.update({4, :disk_table_removed, "foo"})

      snapshot = Snapshot.clear_dirt(snapshot)

      assert %{dirt: [], sequence: 42, wal: "wal.goblin", order: 4} = snapshot
    end
  end
end
