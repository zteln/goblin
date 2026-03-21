defmodule Goblin.Manifest.SnapshotTest do
  use ExUnit.Case, async: true
  alias Goblin.Manifest.Snapshot

  describe "update/2" do
    test "adding disk tables increases count" do
      snapshot = %Snapshot{}
      assert %{count: 0} = snapshot
      assert %{count: 1} = Snapshot.update(snapshot, {:disk_table_activated, "foo"})
    end

    test "removing files adds them to dirt" do
      snapshot = %Snapshot{}
      assert %{dirt: []} = snapshot

      assert %{dirt: ["foo"]} =
               snapshot = Snapshot.update(snapshot, {:disk_table_removed, "foo"})

      assert %{dirt: ["wal", "foo"]} = Snapshot.update(snapshot, {:wal_removed, "wal"})
    end

    test "setting seq updates the seq field" do
      snapshot = %Snapshot{}
      assert %{seq: 0} = snapshot

      assert %{seq: 42} = Snapshot.update(snapshot, {:seq_set, 42})
    end

    test "setting wal updates the wal field" do
      snapshot = %Snapshot{}
      assert %{active_wal: nil} = snapshot

      assert %{active_wal: "wal_0.goblin"} =
               Snapshot.update(snapshot, {:wal_activated, "wal_0.goblin"})
    end

    test "retiring a wal adds it to retired_wals" do
      snapshot = %Snapshot{}
      assert %{retired_wals: []} = snapshot

      snapshot = Snapshot.update(snapshot, {:wal_retired, "wal_0.goblin"})
      assert %{retired_wals: ["wal_0.goblin"]} = snapshot

      snapshot = Snapshot.update(snapshot, {:wal_retired, "wal_1.goblin"})
      assert %{retired_wals: ["wal_1.goblin", "wal_0.goblin"]} = snapshot
    end

    test "applying a list of updates processes all" do
      snapshot =
        Snapshot.update(%Snapshot{}, [
          {:seq_set, 10},
          {:wal_activated, "wal.goblin"},
          {:disk_table_activated, "sst_0.goblin"}
        ])

      assert %{seq: 10, active_wal: "wal.goblin", active_disk_tables: ["sst_0.goblin"], count: 1} =
               snapshot
    end

    test "snapshot term replaces the entire snapshot" do
      original = Snapshot.update(%Snapshot{}, {:seq_set, 99})

      replacement = %Snapshot{seq: 5, active_wal: "other.goblin"}
      result = Snapshot.update(original, {:snapshot, replacement})

      assert result == replacement
    end
  end
end
