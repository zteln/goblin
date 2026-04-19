defmodule Goblin.Mem.TableTest do
  use ExUnit.Case, async: true

  alias Goblin.Mem.Table

  setup do
    %{table: Table.new()}
  end

  describe "insert/4 and get/3" do
    test "inserts and retrieves a value", ctx do
      :ok = Table.insert(ctx.table, :key, 0, "value")

      assert {:key, 0, "value"} == Table.get(ctx.table, :key, 0)
    end

    test "multiple versions of the same key are independently retrievable", ctx do
      :ok = Table.insert(ctx.table, :key, 0, "v0")
      :ok = Table.insert(ctx.table, :key, 1, "v1")
      :ok = Table.insert(ctx.table, :key, 2, "v2")

      assert {:key, 0, "v0"} == Table.get(ctx.table, :key, 0)
      assert {:key, 1, "v1"} == Table.get(ctx.table, :key, 1)
      assert {:key, 2, "v2"} == Table.get(ctx.table, :key, 2)
    end

    test "returns :not_found for missing key", ctx do
      assert :not_found == Table.get(ctx.table, :missing, 0)
    end
  end

  describe "remove/3" do
    test "inserts a tombstone marker", ctx do
      :ok = Table.insert(ctx.table, :key, 0, "value")
      :ok = Table.remove(ctx.table, :key, 1)

      assert {:key, 1, :"$goblin_tombstone"} == Table.get(ctx.table, :key, 1)
    end
  end

  describe "search/3" do
    test "finds the latest version before the given seq", ctx do
      :ok = Table.insert(ctx.table, :key, 0, "v0")
      :ok = Table.insert(ctx.table, :key, 1, "v1")
      :ok = Table.insert(ctx.table, :key, 5, "v5")

      # seq acts as exclusive upper bound
      assert {:key, 1, "v1"} == Table.search(ctx.table, :key, 5)
      assert {:key, 0, "v0"} == Table.search(ctx.table, :key, 1)
    end

    test "returns :not_found when no version exists below seq", ctx do
      :ok = Table.insert(ctx.table, :key, 5, "v5")

      assert :not_found == Table.search(ctx.table, :key, 5)
      assert :not_found == Table.search(ctx.table, :missing, 10)
    end
  end

  describe "has_key?/2" do
    test "returns true for inserted key", ctx do
      :ok = Table.insert(ctx.table, :key, 0, "value")

      assert Table.has_key?(ctx.table, :key)
    end

    test "returns false for missing key", ctx do
      refute Table.has_key?(ctx.table, :missing)
    end
  end

  describe "size/1" do
    test "starts at zero for a fresh table", ctx do
      assert 0 == Table.size(ctx.table)
    end

    test "increases after inserting data", ctx do
      :ok = Table.insert(ctx.table, :key, 0, String.duplicate("x", 100))

      assert Table.size(ctx.table) > 0
    end
  end

  describe "delete/1" do
    test "deletes the underlying ETS table", ctx do
      table_id = ctx.table.ref
      assert :ets.info(table_id) != :undefined

      :ok = Table.delete(ctx.table)

      assert :ets.info(table_id) == :undefined
    end
  end
end
