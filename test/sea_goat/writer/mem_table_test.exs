defmodule SeaGoat.Writer.MemTableTest do
  use ExUnit.Case, async: true
  alias SeaGoat.Writer.MemTable

  test "upsert/3 insert new key" do
    assert %{"key" => "value"} == MemTable.upsert(MemTable.new(), "key", "value")
  end

  test "upsert/3 updates existing key" do
    assert %{"key" => "value"} = mem_table = MemTable.upsert(MemTable.new(), "key", "value")
    assert %{"key" => "value1"} = MemTable.upsert(mem_table, "key", "value1")
  end

  test "upsert/3 does not upsert :tombstone" do
    assert %{} == MemTable.upsert(MemTable.new(), "key", :tombstone)
  end

  test "delete/2 puts value with :tombstone" do
    assert %{"key" => :tombstone} == MemTable.delete(MemTable.new(), "key")
  end

  test "delete/2 replaces value with :tombstone" do
    assert %{"key" => "value"} = mem_table = MemTable.upsert(MemTable.new(), "key", "value")
    assert %{"key" => :tombstone} == MemTable.delete(mem_table, "key")
  end

  test "read/2 returns value" do
    assert %{"key" => "value"} = mem_table = MemTable.upsert(MemTable.new(), "key", "value")
    assert {:value, "value"} == MemTable.read(mem_table, "key")
  end

  test "read/2 returns :not_found if no value set" do
    assert :not_found == MemTable.read(MemTable.new(), "key")
  end

  test "has_overflow/2 returns boolean on overflow" do
    assert false == MemTable.has_overflow(MemTable.new(), 2)

    mem_table =
      MemTable.new()
      |> MemTable.upsert("v1", "k1")
      |> MemTable.upsert("v2", "k2")
      |> MemTable.upsert("v3", "k3")

    assert true == MemTable.has_overflow(mem_table, 2)
  end

  test "is_disjoint/2 returns boolean" do
    assert true == MemTable.is_disjoint(MemTable.new(), MemTable.new())

    mem_table1 =
      MemTable.new()
      |> MemTable.upsert("v1", "k1")
      |> MemTable.upsert("v2", "k2")
      |> MemTable.upsert("v3", "k3")

    mem_table2 =
      MemTable.new()
      |> MemTable.upsert("v2", "k2")
      |> MemTable.upsert("v3", "k3")
      |> MemTable.upsert("v4", "k4")

    mem_table3 =
      MemTable.new()
      |> MemTable.upsert("v4", "k4")
      |> MemTable.upsert("v5", "k5")
      |> MemTable.upsert("v6", "k6")

    assert false == MemTable.is_disjoint(mem_table1, mem_table1)
    assert true == MemTable.is_disjoint(mem_table1, mem_table3)
    assert false == MemTable.is_disjoint(mem_table1, mem_table2)
    assert false == MemTable.is_disjoint(mem_table2, mem_table3)
  end

  test "merge/2 merges to mem_tables" do
    mem_table1 =
      MemTable.new()
      |> MemTable.upsert("v1", "k1")
      |> MemTable.upsert("v2", "k2")
      |> MemTable.upsert("v3", "k3")

    mem_table2 =
      MemTable.new()
      |> MemTable.upsert("v4", "k4")
      |> MemTable.upsert("v5", "k5")
      |> MemTable.upsert("v6", "k6")

    assert %{
             "v1" => "k1",
             "v2" => "k2",
             "v3" => "k3",
             "v4" => "k4",
             "v5" => "k5",
             "v6" => "k6"
           } == MemTable.merge(mem_table1, mem_table2)
  end

  # setup do
  #   %{mem_table: MemTable.new()}
  # end
  #
  # describe "upsert/3" do
  #   test "inserts key-value pair", c do
  #     mem_table = MemTable.upsert(c.mem_table, :key, :value)
  #     assert {:value, :value} == :gb_trees.lookup(:key, mem_table)
  #   end
  #
  #   test "updates key_value pair", c do
  #     mem_table = MemTable.upsert(c.mem_table, :key, :value)
  #     assert {:value, :value} == :gb_trees.lookup(:key, mem_table)
  #     mem_table = MemTable.upsert(c.mem_table, :key, :new_value)
  #     assert {:value, :new_value} == :gb_trees.lookup(:key, mem_table)
  #   end
  #
  #   test "with value :tombstone has no effect", c do
  #     mem_table = MemTable.upsert(c.mem_table, :key, :tombstone)
  #     assert mem_table == c.mem_table
  #   end
  # end
  #
  # describe "delete/2" do
  #   test "does not return value upon read", c do
  #     mem_table = MemTable.upsert(c.mem_table, :key, :value)
  #     assert {:value, :value} == :gb_trees.lookup(:key, mem_table)
  #     mem_table = MemTable.delete(c.mem_table, :key)
  #     assert {:value, :tombstone} == :gb_trees.lookup(:key, mem_table)
  #   end
  # end
  #
  # describe "read/2" do
  #   test "reads inserted value", c do
  #     mem_table = MemTable.upsert(c.mem_table, :key, :value)
  #     assert {:value, :value} == MemTable.read(mem_table, :key)
  #   end
  #
  #   test "does not read deleted key", c do
  #     mem_table = MemTable.upsert(c.mem_table, :key, :value)
  #     mem_table = MemTable.delete(mem_table, :key)
  #     assert nil == MemTable.read(mem_table, :key)
  #   end
  # end
  #
  # describe "reduce/3" do
  #   setup c do
  #     mem_table =
  #       for n <- 0..10, reduce: c.mem_table do
  #         acc ->
  #           MemTable.upsert(acc, n, "v-#{n}")
  #       end
  #
  #     %{mem_table: mem_table}
  #   end
  #
  #   test "reduces over entries", c do
  #     assert for(n <- 10..0//-1, do: {n, "v-#{n}"}) ==
  #              MemTable.reduce(c.mem_table, [], fn k, v, acc ->
  #                {:ok, [{k, v} | acc]}
  #              end)
  #   end
  #
  #   test "halts when reducer returns {:halt, term()}", c do
  #     assert for(n <- 5..0//-1, do: {n, "v-#{n}"}) ==
  #              MemTable.reduce(c.mem_table, [], fn k, v, acc ->
  #                if k == 5 do
  #                  {:halt, [{k, v} | acc]}
  #                else
  #                  {:ok, [{k, v} | acc]}
  #                end
  #              end)
  #   end
  # end
  #
  # describe "smallest/1" do
  #   test "returns smallest key", c do
  #     mem_table =
  #       for n <- 0..10, reduce: c.mem_table do
  #         acc ->
  #           MemTable.upsert(acc, n, "v-#{n}")
  #       end
  #
  #     assert 0 == MemTable.smallest(mem_table)
  #   end
  # end
  #
  # describe "largest/1" do
  #   test "returns largest key", c do
  #     mem_table =
  #       for n <- 0..10, reduce: c.mem_table do
  #         acc ->
  #           MemTable.upsert(acc, n, "v-#{n}")
  #       end
  #
  #     assert 10 == MemTable.largest(mem_table)
  #   end
  # end
  #
  # describe "keys/1" do
  #   test "returns keys in sorted order", c do
  #     mem_table =
  #       for n <- 0..10, reduce: c.mem_table do
  #         acc ->
  #           MemTable.upsert(acc, n, "v-#{n}")
  #       end
  #
  #     assert Enum.to_list(0..10) == MemTable.keys(mem_table)
  #   end
  # end
  #
  # describe "size/1" do
  #   test "returns mem_table size", c do
  #     mem_table =
  #       for n <- 0..10, reduce: c.mem_table do
  #         acc ->
  #           MemTable.upsert(acc, n, "v-#{n}")
  #       end
  #
  #     assert 11 == MemTable.size(mem_table)
  #   end
  # end
  #
  # describe "has_overflow/2" do
  #   test "returns mem_table size", c do
  #     mem_table =
  #       for n <- 0..10, reduce: c.mem_table do
  #         acc ->
  #           MemTable.upsert(acc, n, "v-#{n}")
  #       end
  #
  #     assert false == MemTable.has_overflow(mem_table, 20)
  #     assert true == MemTable.has_overflow(mem_table, 10)
  #   end
  # end
end
