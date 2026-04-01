defmodule Goblin.MemTable.StoreTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable.Store

  setup do
    %{store: Store.new()}
  end

  describe "insert/4 and get/3" do
    test "inserts and retrieves a value", ctx do
      :ok = Store.insert(ctx.store, :key, 0, "value")

      assert {:key, 0, "value"} == Store.get(ctx.store, :key, 0)
    end

    test "multiple versions of the same key are independently retrievable", ctx do
      :ok = Store.insert(ctx.store, :key, 0, "v0")
      :ok = Store.insert(ctx.store, :key, 1, "v1")
      :ok = Store.insert(ctx.store, :key, 2, "v2")

      assert {:key, 0, "v0"} == Store.get(ctx.store, :key, 0)
      assert {:key, 1, "v1"} == Store.get(ctx.store, :key, 1)
      assert {:key, 2, "v2"} == Store.get(ctx.store, :key, 2)
    end

    test "returns :not_found for missing key", ctx do
      assert :not_found == Store.get(ctx.store, :missing, 0)
    end
  end

  describe "remove/3" do
    test "inserts a tombstone marker", ctx do
      :ok = Store.insert(ctx.store, :key, 0, "value")
      :ok = Store.remove(ctx.store, :key, 1)

      assert {:key, 1, :"$goblin_tombstone"} == Store.get(ctx.store, :key, 1)
    end
  end

  describe "search/3" do
    test "finds the latest version before the given seq", ctx do
      :ok = Store.insert(ctx.store, :key, 0, "v0")
      :ok = Store.insert(ctx.store, :key, 1, "v1")
      :ok = Store.insert(ctx.store, :key, 5, "v5")

      # seq acts as exclusive upper bound
      assert {:key, 1, "v1"} == Store.search(ctx.store, :key, 5)
      assert {:key, 0, "v0"} == Store.search(ctx.store, :key, 1)
    end

    test "returns :not_found when no version exists below seq", ctx do
      :ok = Store.insert(ctx.store, :key, 5, "v5")

      assert :not_found == Store.search(ctx.store, :key, 5)
      assert :not_found == Store.search(ctx.store, :missing, 10)
    end
  end

  describe "has_key?/2" do
    test "returns true for inserted key", ctx do
      :ok = Store.insert(ctx.store, :key, 0, "value")

      assert Store.has_key?(ctx.store, :key)
    end

    test "returns false for missing key", ctx do
      refute Store.has_key?(ctx.store, :missing)
    end
  end

  describe "size/1" do
    test "starts at zero for a fresh table", ctx do
      assert 0 == Store.size(ctx.store)
    end

    test "increases after inserting data", ctx do
      :ok = Store.insert(ctx.store, :key, 0, String.duplicate("x", 100))

      assert Store.size(ctx.store) > 0
    end
  end

  describe "delete/1" do
    test "deletes the underlying ETS table", ctx do
      table_id = ctx.store.ref
      assert :ets.info(table_id) != :undefined

      :ok = Store.delete(ctx.store)

      assert :ets.info(table_id) == :undefined
    end
  end
end
