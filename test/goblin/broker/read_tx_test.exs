defmodule Goblin.Broker.ReadTxTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper

  alias Goblin.Broker.{ReadTx, SnapshotRegistry}
  alias Goblin.MemTables

  setup_db()

  @broker __MODULE__.Broker
  @mem_tables __MODULE__.MemTables

  describe "read-only enforcement" do
    test "raises on any write operation" do
      snapshot_ref = Goblin.Broker.SnapshotRegistry.new_ref(@broker)
      tx = Goblin.Broker.ReadTx.new(@broker, snapshot_ref, 0, 0)

      assert_raise RuntimeError, fn ->
        Goblin.Tx.put(tx, :key, :val)
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.put_multi(tx, [{:key1, :val1}, {:key2, :val2}])
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.remove(tx, :key)
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.remove_multi(tx, [:key1, :key2])
      end
    end
  end

  describe "snapshot isolation" do
    test "cannot read writes committed after transaction start", c do
      # write data before transaction
      Goblin.put(c.db, :key1, :val1)

      # capture seq and register snapshot
      seq = MemTables.get_sequence(@mem_tables)
      snapshot_ref = SnapshotRegistry.new_ref(@broker)
      max_level_key = SnapshotRegistry.register_snapshot(@broker, snapshot_ref)
      tx = ReadTx.new(@broker, snapshot_ref, seq, max_level_key)

      # can read data written before transaction
      assert :val1 == Goblin.Tx.get(tx, :key1)
      assert [{:key1, :val1}] == Goblin.Tx.get_multi(tx, [:key1, :key2])

      # write data after transaction
      Goblin.put(c.db, :key2, :val2)

      # cannot read data written after transaction
      assert nil == Goblin.Tx.get(tx, :key2)
      assert [{:key1, :val1}] == Goblin.Tx.get_multi(tx, [:key1, :key2])

      SnapshotRegistry.unregister_snapshot(@broker, snapshot_ref)
    end
  end

  describe "tombstones" do
    test "deleted keys return nil", c do
      # put then remove
      Goblin.put(c.db, :key, :val)
      Goblin.remove(c.db, :key)

      # create read tx at current seq
      seq = MemTables.get_sequence(@mem_tables)
      snapshot_ref = SnapshotRegistry.new_ref(@broker)
      max_level_key = SnapshotRegistry.register_snapshot(@broker, snapshot_ref)
      tx = ReadTx.new(@broker, snapshot_ref, seq, max_level_key)

      # tombstone is transparent — returns nil, not :"$goblin_tombstone"
      assert nil == Goblin.Tx.get(tx, :key)
      assert [] == Goblin.Tx.get_multi(tx, [:key])

      SnapshotRegistry.unregister_snapshot(@broker, snapshot_ref)
    end
  end

  describe "tags" do
    test "reads respect tag namespace", c do
      Goblin.put(c.db, :key, :val, tag: :t1)

      # create read tx at current seq
      seq = MemTables.get_sequence(@mem_tables)
      snapshot_ref = SnapshotRegistry.new_ref(@broker)
      max_level_key = SnapshotRegistry.register_snapshot(@broker, snapshot_ref)
      tx = ReadTx.new(@broker, snapshot_ref, seq, max_level_key)

      # untagged read does not see tagged key
      assert nil == Goblin.Tx.get(tx, :key)
      assert [] == Goblin.Tx.get_multi(tx, [:key])

      # correct tag returns value
      assert :val == Goblin.Tx.get(tx, :key, tag: :t1)
      assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key], tag: :t1)

      # wrong tag returns nil
      assert nil == Goblin.Tx.get(tx, :key, tag: :t2)
      assert [] == Goblin.Tx.get_multi(tx, [:key], tag: :t2)

      SnapshotRegistry.unregister_snapshot(@broker, snapshot_ref)
    end
  end
end
