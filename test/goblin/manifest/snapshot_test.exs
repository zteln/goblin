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
  end
end
