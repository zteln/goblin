defmodule Goblin.Manifest.SnapshotTest do
  use ExUnit.Case, async: true
  alias Goblin.Manifest.Snapshot

  describe "update/2" do
    test "adding disk tables increases count" do
      snapshot = %Snapshot{}
      assert %{count: 0} = snapshot
      assert %{count: 1} = Snapshot.update(snapshot, {:disk_table_added, "foo"})
    end

    test "removing files adds them to tracked" do
      snapshot = %Snapshot{}
      assert %{tracked: []} = snapshot

      assert %{tracked: ["foo"]} =
               snapshot = Snapshot.update(snapshot, {:disk_table_removed, "foo"})

      assert %{tracked: ["wal", "foo"]} = Snapshot.update(snapshot, {:wal_removed, "wal"})
    end

    test "setting seq updates the seq field" do
      snapshot = %Snapshot{}
      assert %{seq: 0} = snapshot

      assert %{seq: 42} = Snapshot.update(snapshot, {:seq, 42})
    end

    test "setting wal updates the wal field" do
      snapshot = %Snapshot{}
      assert %{wal: nil} = snapshot

      assert %{wal: "wal_0.goblin"} = Snapshot.update(snapshot, {:wal_set, "wal_0.goblin"})
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
          {:seq, 10},
          {:wal_set, "wal.goblin"},
          {:disk_table_added, "sst_0.goblin"}
        ])

      assert %{seq: 10, wal: "wal.goblin", disk_tables: ["sst_0.goblin"], count: 1} = snapshot
    end

    test "snapshot term replaces the entire snapshot" do
      original = Snapshot.update(%Snapshot{}, {:seq, 99})

      replacement = %Snapshot{seq: 5, wal: "other.goblin"}
      result = Snapshot.update(original, {:snapshot, replacement})

      assert result == replacement
    end
  end
end
