defmodule Talon.Writer.MemTableTest do
  use ExUnit.Case, async: true
  alias Talon.Writer.MemTable

  test "new/0 creates empty mem table" do
    assert %{} == MemTable.new()
  end

  test "upsert/3 insert new key" do
    assert %{"key" => {0, "value"}} == MemTable.upsert(MemTable.new(), 0, "key", "value")
  end

  test "upsert/3 updates existing key" do
    assert %{"key" => {0, "value"}} =
             mem_table = MemTable.upsert(MemTable.new(), 0, "key", "value")

    assert %{"key" => {1, "value1"}} = MemTable.upsert(mem_table, 1, "key", "value1")
  end

  test "upsert/3 does not upsert :tombstone" do
    assert %{} == MemTable.upsert(MemTable.new(), 0, "key", :tombstone)
  end

  test "delete/2 puts value with :tombstone" do
    assert %{"key" => {0, :tombstone}} == MemTable.delete(MemTable.new(), 0, "key")
  end

  test "delete/2 replaces value with :tombstone" do
    assert %{"key" => {0, "value"}} =
             mem_table = MemTable.upsert(MemTable.new(), 0, "key", "value")

    assert %{"key" => {1, :tombstone}} == MemTable.delete(mem_table, 1, "key")
  end

  test "read/2 returns value" do
    assert %{"key" => {0, "value"}} =
             mem_table = MemTable.upsert(MemTable.new(), 0, "key", "value")

    assert {:value, 0, "value"} == MemTable.read(mem_table, "key")
  end

  test "read/2 returns :not_found if no value set" do
    assert :not_found == MemTable.read(MemTable.new(), "key")
  end

  test "read/2 returns {:value, seq, nil} for deleted key" do
    mem_table =
      MemTable.new()
      |> MemTable.upsert(0, "key", "value")
      |> MemTable.delete(1, "key")

    assert {:value, 1, nil} == MemTable.read(mem_table, "key")
  end

  test "has_overflow/2 returns boolean on overflow" do
    assert false == MemTable.has_overflow(MemTable.new(), 2)

    mem_table =
      MemTable.new()
      |> MemTable.upsert(0, "v1", "k1")
      |> MemTable.upsert(1, "v2", "k2")
      |> MemTable.upsert(2, "v3", "k3")

    assert true == MemTable.has_overflow(mem_table, 2)
  end

  test "is_disjoint/2 returns boolean" do
    assert true == MemTable.is_disjoint(MemTable.new(), MemTable.new())

    mem_table1 =
      MemTable.new()
      |> MemTable.upsert(0, "k1", "v1")
      |> MemTable.upsert(1, "k2", "v2")
      |> MemTable.upsert(2, "k3", "v3")

    mem_table2 =
      MemTable.new()
      |> MemTable.upsert(3, "k2", "v2")
      |> MemTable.upsert(4, "k3", "v3")
      |> MemTable.upsert(5, "k4", "v4")

    mem_table3 =
      MemTable.new()
      |> MemTable.upsert(6, "k4", "v4")
      |> MemTable.upsert(7, "k5", "v5")
      |> MemTable.upsert(8, "k6", "v6")

    assert false == MemTable.is_disjoint(mem_table1, mem_table1)
    assert true == MemTable.is_disjoint(mem_table1, mem_table3)
    assert false == MemTable.is_disjoint(mem_table1, mem_table2)
    assert false == MemTable.is_disjoint(mem_table2, mem_table3)
  end

  test "merge/2 merges to mem_tables" do
    mem_table1 =
      MemTable.new()
      |> MemTable.upsert(0, "k1", "v1")
      |> MemTable.upsert(1, "k2", "v2")
      |> MemTable.upsert(2, "k3", "v3")

    mem_table2 =
      MemTable.new()
      |> MemTable.upsert(3, "k4", "v4")
      |> MemTable.upsert(4, "k5", "v5")
      |> MemTable.upsert(5, "k6", "v6")

    assert %{
             "k1" => {0, "v1"},
             "k2" => {1, "v2"},
             "k3" => {2, "v3"},
             "k4" => {3, "v4"},
             "k5" => {4, "v5"},
             "k6" => {5, "v6"}
           } == MemTable.merge(mem_table1, mem_table2)
  end

  test "merge/2 with empty tables" do
    mem_table = MemTable.new() |> MemTable.upsert(0, "key", "value")

    assert mem_table == MemTable.merge(MemTable.new(), mem_table)
    assert mem_table == MemTable.merge(mem_table, MemTable.new())
    assert %{} == MemTable.merge(MemTable.new(), MemTable.new())
  end

  test "merge/2 overwrites keys from first table" do
    mem_table1 =
      MemTable.new()
      |> MemTable.upsert(0, "key1", "old_value")
      |> MemTable.upsert(1, "key2", "value2")

    mem_table2 =
      MemTable.new()
      |> MemTable.upsert(2, "key1", "new_value")
      |> MemTable.upsert(3, "key3", "value3")

    assert %{
             "key1" => {2, "new_value"},
             "key2" => {1, "value2"},
             "key3" => {3, "value3"}
           } == MemTable.merge(mem_table1, mem_table2)
  end

  test "merge/2 handles tombstones" do
    mem_table1 =
      MemTable.new()
      |> MemTable.upsert(0, "key1", "value1")
      |> MemTable.delete(1, "key2")

    mem_table2 =
      MemTable.new()
      |> MemTable.upsert(2, "key2", "value2")
      |> MemTable.delete(3, "key3")

    assert %{
             "key1" => {0, "value1"},
             "key2" => {2, "value2"},
             "key3" => {3, :tombstone}
           } == MemTable.merge(mem_table1, mem_table2)
  end
end
