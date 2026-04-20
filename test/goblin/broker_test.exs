defmodule Goblin.BrokerTest do
  use ExUnit.Case, async: true

  alias Goblin.Broker

  defp add_fake(ref, id, level_key, on_remove \\ fn _ -> :ok end) do
    table = %{id: id, level_key: level_key}
    Broker.add_table(ref, id, level_key, table, on_remove)
    table
  end

  setup do
    ref = Broker.new()
    %{ref: ref}
  end

  describe "add_table/5 and filter_tables/3" do
    test "added table is visible to a registered transaction", ctx do
      t1 = add_fake(ctx.ref, :t1, 0)

      tx_key = make_ref()
      {_, _, tx_id} = Broker.register_tx(ctx.ref, tx_key)

      assert [t1] == Broker.filter_tables(ctx.ref, tx_id)
    end

    test "filter option excludes non-matching tables", ctx do
      t1 = add_fake(ctx.ref, :t1, 0)
      _t2 = add_fake(ctx.ref, :t2, 0)

      tx_key = make_ref()
      {_, _, tx_id} = Broker.register_tx(ctx.ref, tx_key)

      result = Broker.filter_tables(ctx.ref, tx_id, filter: &(&1.id == :t1))
      assert result == [t1]
    end

    test "level_key option filters by level", ctx do
      t1 = add_fake(ctx.ref, :t1, 0)
      t2 = add_fake(ctx.ref, :t2, 1)

      tx_key = make_ref()
      {_, _, tx_id} = Broker.register_tx(ctx.ref, tx_key)

      assert [t1] == Broker.filter_tables(ctx.ref, tx_id, level_key: 0)
      assert [t2] == Broker.filter_tables(ctx.ref, tx_id, level_key: 1)
    end
  end

  describe "soft_delete_table/2 and hard_delete_table/2" do
    test "hard_delete invokes the remover when no snapshots reference table", ctx do
      parent = self()
      add_fake(ctx.ref, :t1, 0, fn _ -> send(parent, :deleted) end)

      Broker.soft_delete_table(ctx.ref, :t1)
      assert :ok == Broker.hard_delete_table(ctx.ref, :t1)

      assert_receive :deleted
    end

    test "hard_delete does not invoke the remover while snapshot holds reference", ctx do
      parent = self()
      add_fake(ctx.ref, :t1, 0, fn _ -> send(parent, :deleted) end)

      tx_key = make_ref()
      Broker.register_tx(ctx.ref, tx_key)

      Broker.soft_delete_table(ctx.ref, :t1)
      assert :ok == Broker.hard_delete_table(ctx.ref, :t1)

      refute_receive :deleted

      # After unregistering, hard_delete should succeed
      Broker.unregister_tx(ctx.ref, tx_key)
      assert :ok == Broker.hard_delete_table(ctx.ref, :t1)

      assert_receive :deleted
    end

    test "hard_delete returns error after exceeding max retries", ctx do
      add_fake(ctx.ref, :t1, 0)

      # Create a snapshot that holds the reference
      tx_key = make_ref()
      Broker.register_tx(ctx.ref, tx_key)

      Broker.soft_delete_table(ctx.ref, :t1)

      # Retry 61 times to exceed the @max_retries (60) limit
      for _ <- 1..61 do
        Broker.hard_delete_table(ctx.ref, :t1)
      end

      assert {:error, :too_many_retries} == Broker.hard_delete_table(ctx.ref, :t1)
    end

    test "soft_delete of nonexistent table is a no-op", ctx do
      assert :ok == Broker.soft_delete_table(ctx.ref, :nonexistent)
    end
  end

  describe "hard_delete_tables/1" do
    test "deletes all eligible soft-deleted tables", ctx do
      parent = self()
      add_fake(ctx.ref, :t1, 0, fn _ -> send(parent, :deleted_t1) end)
      add_fake(ctx.ref, :t2, 0, fn _ -> send(parent, :deleted_t2) end)

      Broker.soft_delete_table(ctx.ref, :t1)
      Broker.soft_delete_table(ctx.ref, :t2)

      Broker.hard_delete_tables(ctx.ref)

      assert_receive :deleted_t1
      assert_receive :deleted_t2
    end

    test "skips tables held by active transaction", ctx do
      parent = self()
      add_fake(ctx.ref, :t1, 0, fn _ -> send(parent, :deleted_t1) end)
      add_fake(ctx.ref, :t2, 0, fn _ -> send(parent, :deleted_t2) end)

      tx_key = make_ref()
      Broker.register_tx(ctx.ref, tx_key)

      Broker.soft_delete_table(ctx.ref, :t1)
      Broker.soft_delete_table(ctx.ref, :t2)

      Broker.hard_delete_tables(ctx.ref)

      refute_receive :deleted_t1
      refute_receive :deleted_t2
    end
  end

  describe "register_tx/2 and unregister_tx/2" do
    test "returns max_level_key and seq", ctx do
      add_fake(ctx.ref, :t1, 0)
      add_fake(ctx.ref, :t2, 2)
      Broker.put_sequence(ctx.ref, 42)

      tx_key = make_ref()
      assert {2, 42, _tx_id} = Broker.register_tx(ctx.ref, tx_key)
    end

    test "returns {-1, 0, _} when no tables exist", ctx do
      tx_key = make_ref()
      assert {-1, 0, _tx_id} = Broker.register_tx(ctx.ref, tx_key)
    end

    test "unregister_tx removes snapshot entries", ctx do
      t1 = add_fake(ctx.ref, :t1, 0)

      tx_key = make_ref()
      {_, _, tx_id} = Broker.register_tx(ctx.ref, tx_key)

      assert [t1] == Broker.filter_tables(ctx.ref, tx_id)

      Broker.unregister_tx(ctx.ref, tx_key)

      assert :ets.match(ctx.ref, {{:_, :tx, tx_key}}) == []
    end
  end

  describe "put_sequence/2" do
    test "stores and retrieves sequence number", ctx do
      Broker.put_sequence(ctx.ref, 10)

      tx_key = make_ref()
      {_max_level_key, seq, _tx_id} = Broker.register_tx(ctx.ref, tx_key)

      assert seq == 10
    end

    test "updates existing sequence number", ctx do
      Broker.put_sequence(ctx.ref, 5)
      Broker.put_sequence(ctx.ref, 15)

      tx_key = make_ref()
      {_max_level_key, seq, _tx_id} = Broker.register_tx(ctx.ref, tx_key)

      assert seq == 15
    end
  end
end
