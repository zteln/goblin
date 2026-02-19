defmodule Goblin.Broker.SnapshotRegistryTest do
  use ExUnit.Case, async: true

  alias Goblin.Broker.SnapshotRegistry

  setup do
    snapshot_registry = SnapshotRegistry.new(__MODULE__)
    SnapshotRegistry.inc_ready(snapshot_registry)
    SnapshotRegistry.inc_ready(snapshot_registry)
    %{snapshot_registry: snapshot_registry}
  end

  describe "snapshot isolation" do
    test "registered snapshot sees active table, not soft-deleted tables", c do
      table1 = %{id: "table1"}
      table2 = %{id: "table2"}
      # add tables
      SnapshotRegistry.add_table(
        c.snapshot_registry,
        table1.id,
        0,
        table1,
        fn _ -> :ok end
      )

      SnapshotRegistry.add_table(
        c.snapshot_registry,
        table2.id,
        0,
        table2,
        fn _ -> :ok end
      )

      # register first snapshot before soft delete
      ref1 = SnapshotRegistry.new_ref(c.snapshot_registry)
      assert 0 == SnapshotRegistry.register_snapshot(c.snapshot_registry, ref1)

      # soft delete a table
      assert :ok == SnapshotRegistry.soft_delete(c.snapshot_registry, "table1")

      # register snapshot after soft delete
      ref2 = SnapshotRegistry.new_ref(c.snapshot_registry)
      assert 0 == SnapshotRegistry.register_snapshot(c.snapshot_registry, ref2)

      assert Enum.sort([table1, table2]) ==
               SnapshotRegistry.filter_tables(c.snapshot_registry, ref1) |> Enum.sort()

      assert Enum.sort([table2]) ==
               SnapshotRegistry.filter_tables(c.snapshot_registry, ref2) |> Enum.sort()
    end
  end

  describe "hard_delete/1" do
    test "deletes inactive tables only when no snapshots reference them", c do
      table = %{id: "table"}

      # add tables
      SnapshotRegistry.add_table(
        c.snapshot_registry,
        table.id,
        0,
        table,
        fn _ -> :ok end
      )

      # register snapshot before soft delete
      ref = SnapshotRegistry.new_ref(c.snapshot_registry)
      assert 0 == SnapshotRegistry.register_snapshot(c.snapshot_registry, ref)

      # soft delete table
      assert :ok == SnapshotRegistry.soft_delete(c.snapshot_registry, "table")

      # try hard delete table
      assert :ok == SnapshotRegistry.hard_delete(c.snapshot_registry)

      assert [table] == SnapshotRegistry.filter_tables(c.snapshot_registry, ref)
      assert :ok == SnapshotRegistry.unregister_snapshot(c.snapshot_registry, ref)

      # try hard delete table
      assert :ok == SnapshotRegistry.hard_delete(c.snapshot_registry)

      # cannot read table anymore
      ref = SnapshotRegistry.new_ref(c.snapshot_registry)
      assert -1 == SnapshotRegistry.register_snapshot(c.snapshot_registry, ref)
      assert [] == SnapshotRegistry.filter_tables(c.snapshot_registry, ref)
    end

    test "returns error after too many retries", c do
      table = %{id: "table"}

      # add table
      SnapshotRegistry.add_table(
        c.snapshot_registry,
        table.id,
        0,
        table,
        fn _ -> :ok end
      )

      # register snapshot before soft delete
      ref = SnapshotRegistry.new_ref(c.snapshot_registry)
      assert 0 == SnapshotRegistry.register_snapshot(c.snapshot_registry, ref)

      # soft delete table
      assert :ok == SnapshotRegistry.soft_delete(c.snapshot_registry, "table")

      # try delete many times
      for _ <- 0..60 do
        assert :ok == SnapshotRegistry.hard_delete(c.snapshot_registry)
      end

      assert {:error, :too_many_retries} == SnapshotRegistry.hard_delete(c.snapshot_registry)
    end
  end

  describe "readiness" do
    test "new_ref/1 blocks until ready flags reach 2" do
      snapshot_registry = SnapshotRegistry.new(__MODULE__.TestReadiness)
      parent = self()

      # spawn caller
      spawn(fn ->
        _ref = SnapshotRegistry.new_ref(snapshot_registry)
        send(parent, :done)
      end)

      # still blocking
      refute_receive :done

      SnapshotRegistry.inc_ready(snapshot_registry)

      # still blocking
      refute_receive :done

      SnapshotRegistry.inc_ready(snapshot_registry)

      # released
      assert_receive :done
    end
  end
end
