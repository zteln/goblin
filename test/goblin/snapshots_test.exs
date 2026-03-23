defmodule Goblin.SnapshotsTest do
  use ExUnit.Case, async: true

  alias Goblin.Snapshots

  setup do
    ref = Snapshots.new()
    %{ref: ref}
  end

  describe "add_table/5 and filter_tables/3" do
    test "added table is visible to a registered transaction", ctx do
      Snapshots.add_table(ctx.ref, :t1, 0, :table_data, fn _ -> :ok end)

      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      assert [:table_data] == Snapshots.filter_tables(ctx.ref, tx_key)
    end

    test "filter option excludes non-matching tables", ctx do
      Snapshots.add_table(ctx.ref, :t1, 0, :keep, fn _ -> :ok end)
      Snapshots.add_table(ctx.ref, :t2, 0, :skip, fn _ -> :ok end)

      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      result = Snapshots.filter_tables(ctx.ref, tx_key, filter: &(&1 == :keep))
      assert result == [:keep]
    end

    test "level_key option filters by level", ctx do
      Snapshots.add_table(ctx.ref, :t1, 0, :level_0, fn _ -> :ok end)
      Snapshots.add_table(ctx.ref, :t2, 1, :level_1, fn _ -> :ok end)

      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      assert [:level_0] == Snapshots.filter_tables(ctx.ref, tx_key, level_key: 0)
      assert [:level_1] == Snapshots.filter_tables(ctx.ref, tx_key, level_key: 1)
    end
  end

  describe "soft_delete_table/2 and hard_delete_table/2" do
    test "hard_delete invokes callback when no snapshots reference table", ctx do
      parent = self()
      Snapshots.add_table(ctx.ref, :t1, 0, :data, fn _ -> send(parent, :deleted) end)

      Snapshots.soft_delete_table(ctx.ref, :t1)
      assert :ok == Snapshots.hard_delete_table(ctx.ref, :t1)

      assert_receive :deleted
    end

    test "hard_delete does not invoke callback while snapshot holds reference", ctx do
      parent = self()
      Snapshots.add_table(ctx.ref, :t1, 0, :data, fn _ -> send(parent, :deleted) end)

      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      Snapshots.soft_delete_table(ctx.ref, :t1)
      assert :ok == Snapshots.hard_delete_table(ctx.ref, :t1)

      refute_receive :deleted

      # After unregistering, hard_delete should succeed
      Snapshots.unregister_tx(ctx.ref, tx_key)
      assert :ok == Snapshots.hard_delete_table(ctx.ref, :t1)

      assert_receive :deleted
    end

    test "hard_delete returns error after exceeding max retries", ctx do
      Snapshots.add_table(ctx.ref, :t1, 0, :data, fn _ -> :ok end)

      # Create a snapshot that holds the reference
      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      Snapshots.soft_delete_table(ctx.ref, :t1)

      # Retry 61 times to exceed the @max_retries (60) limit
      for _ <- 1..61 do
        Snapshots.hard_delete_table(ctx.ref, :t1)
      end

      assert {:error, :too_many_retries} == Snapshots.hard_delete_table(ctx.ref, :t1)
    end

    test "soft_delete of nonexistent table is a no-op", ctx do
      assert :ok == Snapshots.soft_delete_table(ctx.ref, :nonexistent)
    end
  end

  describe "hard_delete_tables/1" do
    test "deletes all eligible soft-deleted tables", ctx do
      parent = self()
      Snapshots.add_table(ctx.ref, :t1, 0, :data1, fn _ -> send(parent, :deleted_t1) end)
      Snapshots.add_table(ctx.ref, :t2, 0, :data2, fn _ -> send(parent, :deleted_t2) end)

      Snapshots.soft_delete_table(ctx.ref, :t1)
      Snapshots.soft_delete_table(ctx.ref, :t2)

      Snapshots.hard_delete_tables(ctx.ref)

      assert_receive :deleted_t1
      assert_receive :deleted_t2
    end

    test "skips tables held by active transaction", ctx do
      parent = self()
      Snapshots.add_table(ctx.ref, :t1, 0, :data1, fn _ -> send(parent, :deleted_t1) end)
      Snapshots.add_table(ctx.ref, :t2, 0, :data2, fn _ -> send(parent, :deleted_t2) end)

      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      Snapshots.soft_delete_table(ctx.ref, :t1)
      Snapshots.soft_delete_table(ctx.ref, :t2)

      Snapshots.hard_delete_tables(ctx.ref)

      refute_receive :deleted_t1
      refute_receive :deleted_t2
    end
  end

  describe "register_tx/2 and unregister_tx/2" do
    test "returns max_level_key and seq", ctx do
      Snapshots.add_table(ctx.ref, :t1, 0, :data1, fn _ -> :ok end)
      Snapshots.add_table(ctx.ref, :t2, 2, :data2, fn _ -> :ok end)
      Snapshots.put_seq(ctx.ref, 42)

      tx_key = make_ref()
      assert {2, 42} == Snapshots.register_tx(ctx.ref, tx_key)
    end

    test "returns {-1, 0} when no tables exist", ctx do
      tx_key = make_ref()
      assert {-1, 0} == Snapshots.register_tx(ctx.ref, tx_key)
    end

    test "unregister_tx removes snapshot entries", ctx do
      Snapshots.add_table(ctx.ref, :t1, 0, :data, fn _ -> :ok end)

      tx_key = make_ref()
      Snapshots.register_tx(ctx.ref, tx_key)

      assert [:data] == Snapshots.filter_tables(ctx.ref, tx_key)

      Snapshots.unregister_tx(ctx.ref, tx_key)

      assert_raise MatchError, fn -> Snapshots.filter_tables(ctx.ref, tx_key) end
    end
  end

  describe "put_seq/2" do
    test "stores and retrieves sequence number", ctx do
      Snapshots.put_seq(ctx.ref, 10)

      tx_key = make_ref()
      {_max_level_key, seq} = Snapshots.register_tx(ctx.ref, tx_key)

      assert seq == 10
    end

    test "updates existing sequence number", ctx do
      Snapshots.put_seq(ctx.ref, 5)
      Snapshots.put_seq(ctx.ref, 15)

      tx_key = make_ref()
      {_max_level_key, seq} = Snapshots.register_tx(ctx.ref, tx_key)

      assert seq == 15
    end
  end
end
