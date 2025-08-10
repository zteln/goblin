defmodule SeaGoat.MemTableTest do
  use ExUnit.Case, async: true
  alias SeaGoat.MemTable

  setup do
    %{mem_table: MemTable.new()}
  end

  describe "upsert/3" do
    test "inserts key-value pair", c do
      mem_table = MemTable.upsert(c.mem_table, :key, :value)
      assert {:value, :value} == :gb_trees.lookup(:key, mem_table)
    end

    test "updates key_value pair", c do
      mem_table = MemTable.upsert(c.mem_table, :key, :value)
      assert {:value, :value} == :gb_trees.lookup(:key, mem_table)
      mem_table = MemTable.upsert(c.mem_table, :key, :new_value)
      assert {:value, :new_value} == :gb_trees.lookup(:key, mem_table)
    end

    test "with value :tombstone has no effect", c do
      mem_table = MemTable.upsert(c.mem_table, :key, :tombstone)
      assert mem_table == c.mem_table
    end
  end

  describe "delete/2" do
    test "does not return value upon read", c do
      mem_table = MemTable.upsert(c.mem_table, :key, :value)
      assert {:value, :value} == :gb_trees.lookup(:key, mem_table)
      mem_table = MemTable.delete(c.mem_table, :key)
      assert {:value, :tombstone} == :gb_trees.lookup(:key, mem_table)
    end
  end

  describe "read/2" do
    test "reads inserted value", c do
      mem_table = MemTable.upsert(c.mem_table, :key, :value)
      assert {:value, :value} == MemTable.read(mem_table, :key)
    end

    test "does not read deleted key", c do
      mem_table = MemTable.upsert(c.mem_table, :key, :value)
      mem_table = MemTable.delete(mem_table, :key)
      assert nil == MemTable.read(mem_table, :key)
    end
  end

  describe "reduce/3" do
    setup c do
      mem_table =
        for n <- 0..10, reduce: c.mem_table do
          acc ->
            MemTable.upsert(acc, n, "v-#{n}")
        end

      %{mem_table: mem_table}
    end

    test "reduces over entries", c do
      assert for(n <- 10..0//-1, do: {n, "v-#{n}"}) ==
               MemTable.reduce(c.mem_table, [], fn k, v, acc ->
                 {:ok, [{k, v} | acc]}
               end)
    end

    test "halts when reducer returns {:halt, term()}", c do
      assert for(n <- 5..0//-1, do: {n, "v-#{n}"}) ==
               MemTable.reduce(c.mem_table, [], fn k, v, acc ->
                 if k == 5 do
                   {:halt, [{k, v} | acc]}
                 else
                   {:ok, [{k, v} | acc]}
                 end
               end)
    end
  end

  describe "smallest/1" do
    test "returns smallest key", c do
      mem_table =
        for n <- 0..10, reduce: c.mem_table do
          acc ->
            MemTable.upsert(acc, n, "v-#{n}")
        end

      assert 0 == MemTable.smallest(mem_table)
    end
  end

  describe "largest/1" do
    test "returns largest key", c do
      mem_table =
        for n <- 0..10, reduce: c.mem_table do
          acc ->
            MemTable.upsert(acc, n, "v-#{n}")
        end

      assert 10 == MemTable.largest(mem_table)
    end
  end

  describe "keys/1" do
    test "returns keys in sorted order", c do
      mem_table =
        for n <- 0..10, reduce: c.mem_table do
          acc ->
            MemTable.upsert(acc, n, "v-#{n}")
        end

      assert Enum.to_list(0..10) == MemTable.keys(mem_table)
    end
  end

  describe "size/1" do
    test "returns mem_table size", c do
      mem_table =
        for n <- 0..10, reduce: c.mem_table do
          acc ->
            MemTable.upsert(acc, n, "v-#{n}")
        end

      assert 11 == MemTable.size(mem_table)
    end
  end

  describe "has_overflow/2" do
    test "returns mem_table size", c do
      mem_table =
        for n <- 0..10, reduce: c.mem_table do
          acc ->
            MemTable.upsert(acc, n, "v-#{n}")
        end

      assert false == MemTable.has_overflow(mem_table, 20)
      assert true == MemTable.has_overflow(mem_table, 10)
    end
  end
end
