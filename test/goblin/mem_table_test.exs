defmodule Goblin.MemTableTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable

  setup do
    %{mem_table: MemTable.new()}
  end

  describe "insert/4 and get/3" do
    test "inserts and retrieves a value", c do
      :ok = MemTable.insert(c.mem_table, :key, 0, "value")

      assert {:key, 0, "value"} == MemTable.get(c.mem_table, :key, 0)
    end

    test "multiple versions of the same key are independently retrievable", c do
      :ok = MemTable.insert(c.mem_table, :key, 0, "v0")
      :ok = MemTable.insert(c.mem_table, :key, 1, "v1")
      :ok = MemTable.insert(c.mem_table, :key, 2, "v2")

      assert {:key, 0, "v0"} == MemTable.get(c.mem_table, :key, 0)
      assert {:key, 1, "v1"} == MemTable.get(c.mem_table, :key, 1)
      assert {:key, 2, "v2"} == MemTable.get(c.mem_table, :key, 2)
    end

    test "returns :not_found for missing key", c do
      assert :not_found == MemTable.get(c.mem_table, :missing, 0)
    end
  end

  describe "remove/3" do
    test "inserts a tombstone marker", c do
      :ok = MemTable.insert(c.mem_table, :key, 0, "value")
      :ok = MemTable.remove(c.mem_table, :key, 1)

      assert {:key, 1, :"$goblin_tombstone"} == MemTable.get(c.mem_table, :key, 1)
    end
  end

  describe "search/3" do
    test "finds the latest version before the given seq", c do
      :ok = MemTable.insert(c.mem_table, :key, 0, "v0")
      :ok = MemTable.insert(c.mem_table, :key, 1, "v1")
      :ok = MemTable.insert(c.mem_table, :key, 5, "v5")

      # seq acts as exclusive upper bound
      assert {:key, 1, "v1"} == MemTable.search(c.mem_table, :key, 5)
      assert {:key, 0, "v0"} == MemTable.search(c.mem_table, :key, 1)
    end

    test "returns :not_found when no version exists below seq", c do
      :ok = MemTable.insert(c.mem_table, :key, 5, "v5")

      assert :not_found == MemTable.search(c.mem_table, :key, 5)
      assert :not_found == MemTable.search(c.mem_table, :missing, 10)
    end
  end

  describe "has_key?/2" do
    test "returns true for inserted key", c do
      :ok = MemTable.insert(c.mem_table, :key, 0, "value")

      assert MemTable.has_key?(c.mem_table, :key)
    end

    test "returns false for missing key", c do
      refute MemTable.has_key?(c.mem_table, :missing)
    end
  end

  describe "size/1" do
    test "starts at zero for a fresh table", c do
      assert 0 == MemTable.size(c.mem_table)
    end

    test "increases after inserting data", c do
      :ok = MemTable.insert(c.mem_table, :key, 0, String.duplicate("x", 100))

      assert MemTable.size(c.mem_table) > 0
    end
  end

  describe "delete/1" do
    test "deletes the underlying ETS table", c do
      table_id = c.mem_table.table
      assert :ets.info(table_id) != :undefined

      :ok = MemTable.delete(c.mem_table)

      assert :ets.info(table_id) == :undefined
    end
  end
end
