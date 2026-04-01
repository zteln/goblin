defmodule Goblin.Manifest.SnapshotTest do
  use ExUnit.Case, async: true
  alias Goblin.Manifest.Snapshot

  describe "update/2" do
    test "adding disk tables increases count" do
      snapshot = %Snapshot{}
      assert %{disk_table_count: 0} = snapshot
      assert %{disk_table_count: 1} = Snapshot.update(snapshot, {:disk_table_added, "foo"})
    end

    test "removing disk tables adds them to dirt" do
      snapshot = %Snapshot{}
      assert %{dirt: []} = snapshot

      snapshot = Snapshot.update(snapshot, {:disk_table_added, "foo"})
      assert %{dirt: ["foo"]} = Snapshot.update(snapshot, {:disk_table_removed, "foo"})
    end

    test "setting sequence updates the sequence field" do
      snapshot = %Snapshot{}
      assert %{sequence: 0} = snapshot

      assert %{sequence: 42} = Snapshot.update(snapshot, {:sequence, 42})
    end

    test "setting wal updates the wal field" do
      snapshot = %Snapshot{}
      assert %{wal: nil} = snapshot

      assert %{wal: "wal_0.goblin"} =
               Snapshot.update(snapshot, {:wal, "wal_0.goblin"})
    end

    test "adding a new wal moves the current wal to wals" do
      snapshot = %Snapshot{}
      assert %{wals: []} = snapshot

      snapshot = Snapshot.update(snapshot, {:wal, "wal_0.goblin"})
      assert %{wal: "wal_0.goblin", wals: []} = snapshot

      snapshot = Snapshot.update(snapshot, {:wal, "wal_1.goblin"})
      assert %{wal: "wal_1.goblin", wals: ["wal_0.goblin"]} = snapshot
    end

    test "wal_removed removes from wals list" do
      snapshot =
        %Snapshot{}
        |> Snapshot.update({:wal, "wal_0.goblin"})
        |> Snapshot.update({:wal, "wal_1.goblin"})

      assert %{wals: ["wal_0.goblin"]} = snapshot

      snapshot = Snapshot.update(snapshot, {:wal_removed, "wal_0.goblin"})
      assert %{wals: []} = snapshot
    end

    test "applying a list of updates processes all" do
      snapshot =
        Snapshot.update(%Snapshot{}, [
          {:sequence, 10},
          {:wal, "wal.goblin"},
          {:disk_table_added, "sst_0.goblin"}
        ])

      assert %{sequence: 10, wal: "wal.goblin", disk_tables: ["sst_0.goblin"], disk_table_count: 1} =
               snapshot
    end

    test "snapshot term replaces the entire snapshot" do
      original = Snapshot.update(%Snapshot{}, {:sequence, 99})

      replacement = %Snapshot{sequence: 5, wal: "other.goblin"}
      result = Snapshot.update(original, {:snapshot, replacement})

      assert result == replacement
    end
  end
end
