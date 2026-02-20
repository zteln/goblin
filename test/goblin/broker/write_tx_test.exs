defmodule Goblin.Broker.WriteTxTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper

  alias Goblin.Broker.{WriteTx, SnapshotRegistry}
  alias Goblin.MemTables

  setup_db()

  @broker __MODULE__.Broker
  @mem_tables __MODULE__.MemTables

  describe "read own writes" do
    test "can read uncommitted puts and removes within transaction", c do
      tx = new_write_tx()

      # put via tx — visible in tx, not in db
      tx = Goblin.Tx.put(tx, :key, :val)
      assert :val == Goblin.Tx.get(tx, :key)
      assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key])
      assert nil == Goblin.get(c.db, :key)

      # remove via tx — no longer visible in tx
      tx = Goblin.Tx.remove(tx, :key)
      assert nil == Goblin.Tx.get(tx, :key)
      assert [] == Goblin.Tx.get_multi(tx, [:key])

      # put_multi via tx
      tx = Goblin.Tx.put_multi(tx, [{:k1, :v1}, {:k2, :v2}])
      assert :v1 == Goblin.Tx.get(tx, :k1)
      assert :v2 == Goblin.Tx.get(tx, :k2)
      assert [{:k1, :v1}, {:k2, :v2}] == Goblin.Tx.get_multi(tx, [:k1, :k2])

      # remove_multi via tx
      tx = Goblin.Tx.remove_multi(tx, [:k1, :k2])
      assert nil == Goblin.Tx.get(tx, :k1)
      assert nil == Goblin.Tx.get(tx, :k2)
      assert [] == Goblin.Tx.get_multi(tx, [:k1, :k2])
    end
  end

  describe "fallback to committed data" do
    test "reads committed data when key is not in transaction writes", c do
      Goblin.put(c.db, :key, :val)

      tx = new_write_tx()

      # tx has no local writes for :key, falls back to committed data
      assert :val == Goblin.Tx.get(tx, :key)
      assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key])
    end
  end

  describe "tags" do
    test "writes and reads respect tag namespace", c do
      # commit a tagged key before tx
      Goblin.put(c.db, :key1, :val1, tag: :t1)

      tx = new_write_tx()

      # write a tagged key within tx
      tx = Goblin.Tx.put(tx, :key2, :val2, tag: :t1)

      # untagged reads don't see tagged keys
      assert nil == Goblin.Tx.get(tx, :key1)
      assert nil == Goblin.Tx.get(tx, :key2)

      # correct tag sees both committed and uncommitted
      assert :val1 == Goblin.Tx.get(tx, :key1, tag: :t1)
      assert :val2 == Goblin.Tx.get(tx, :key2, tag: :t1)
      assert [{:key1, :val1}, {:key2, :val2}] == Goblin.Tx.get_multi(tx, [:key1, :key2], tag: :t1)

      # wrong tag sees neither
      assert nil == Goblin.Tx.get(tx, :key1, tag: :t2)
      assert nil == Goblin.Tx.get(tx, :key2, tag: :t2)
      assert [] == Goblin.Tx.get_multi(tx, [:key1, :key2], tag: :t2)
    end
  end

  defp new_write_tx do
    seq = MemTables.get_sequence(@mem_tables)
    snapshot_ref = SnapshotRegistry.new_ref(@broker)
    max_level_key = SnapshotRegistry.register_snapshot(@broker, snapshot_ref)
    tx = WriteTx.new(@broker, snapshot_ref, seq, max_level_key)
    tx
  end
end
