defmodule Goblin.MemTables.MemTableTest do
  use ExUnit.Case, async: true
  alias Goblin.MemTables.MemTable

  describe "get_by_key/3" do
    test "returns highest seq corresponding to queried key" do
      mem_table = MemTable.new(__MODULE__)

      assert :ok == MemTable.insert(mem_table, :key, 0, :val1)
      assert :ok == MemTable.insert(mem_table, :key, 1, :val2)
      assert :ok == MemTable.insert(mem_table, :key, 2, :val3)

      assert {:key, 2, :val3} == MemTable.get_by_key(mem_table, :key, 3)
    end
  end

  describe "remove/3" do
    test "replaces value with tombstone value" do
      mem_table = MemTable.new(__MODULE__)

      assert :ok == MemTable.insert(mem_table, :key, 0, :val)
      assert :ok == MemTable.remove(mem_table, :key, 1)

      assert {:key, 1, :"$goblin_tombstone"} == MemTable.get_by_key(mem_table, :key, 2)
    end
  end

  describe "iterate/1/2" do
    test "iterates in correct order (key order then sequence order)" do
      mem_table = MemTable.new(__MODULE__)

      assert :ok == MemTable.insert(mem_table, :key1, 0, :val1_1)
      assert :ok == MemTable.insert(mem_table, :key2, 1, :val2_1)
      assert :ok == MemTable.insert(mem_table, :key2, 2, :val2_2)
      assert :ok == MemTable.insert(mem_table, :key3, 3, :val3_1)
      assert :ok == MemTable.insert(mem_table, :key1, 4, :val1_2)

      assert [
               {:key1, 4},
               {:key1, 0},
               {:key2, 2},
               {:key2, 1},
               {:key3, 3}
             ] ==
               Stream.resource(
                 fn ->
                   MemTable.iterate(mem_table)
                 end,
                 fn
                   :end_of_iteration -> {:halt, :ok}
                   {key, seq} -> {[{key, seq}], MemTable.iterate(mem_table, {key, seq})}
                 end,
                 fn _ -> :ok end
               )
               |> Enum.to_list()
    end
  end
end
