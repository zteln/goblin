defmodule Goblin.Broker.WriteTxTest do
  use ExUnit.Case, async: true
  use TestHelper

  setup_db()

  @mem_table __MODULE__.MemTable
  @disk_tables __MODULE__.DiskTables

  test "prepends write to writes" do
    %{writes: []} = tx = Goblin.Broker.WriteTx.new(@mem_table, @disk_tables)
    assert %{writes: [{:put, 0, :key, :val}]} = tx = Goblin.Tx.put(tx, :key, :val)

    assert %{
             writes: [
               {:put, 2, :key2, :val2},
               {:put, 1, :key1, :val1},
               {:put, 0, :key, :val}
             ]
           } =
             tx =
             Goblin.Tx.put_multi(tx, [{:key1, :val1}, {:key2, :val2}])

    assert %{
             writes: [
               {:remove, 3, :key},
               {:put, 2, :key2, :val2},
               {:put, 1, :key1, :val1},
               {:put, 0, :key, :val}
             ]
           } =
             tx =
             Goblin.Tx.remove(tx, :key)

    assert %{
             writes: [
               {:remove, 5, :key2},
               {:remove, 4, :key1},
               {:remove, 3, :key},
               {:put, 2, :key2, :val2},
               {:put, 1, :key1, :val1},
               {:put, 0, :key, :val}
             ]
           } =
             Goblin.Tx.remove_multi(tx, [:key1, :key2])
  end

  test "can read own writes", c do
    tx = Goblin.Broker.WriteTx.new(@mem_table, @disk_tables)

    tx = Goblin.Tx.put(tx, :key, :val)
    assert nil == Goblin.get(c.db, :key)
    assert :val == Goblin.Tx.get(tx, :key)
    assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key])
    assert [{:key, :val}] == Goblin.Tx.select(tx) |> Enum.to_list()

    tx = Goblin.Tx.remove(tx, :key)
    assert nil == Goblin.get(c.db, :key)
    assert nil == Goblin.Tx.get(tx, :key)
    assert [] == Goblin.Tx.get_multi(tx, [:key])
    assert [] == Goblin.Tx.select(tx) |> Enum.to_list()

    tx = Goblin.Tx.put_multi(tx, [{:key1, :val1}, {:key2, :val2}])
    assert nil == Goblin.get(c.db, :key1)
    assert nil == Goblin.get(c.db, :key2)
    assert :val1 == Goblin.Tx.get(tx, :key1)
    assert :val2 == Goblin.Tx.get(tx, :key2)
    assert [{:key1, :val1}, {:key2, :val2}] == Goblin.Tx.get_multi(tx, [:key1, :key2])
    assert [{:key1, :val1}, {:key2, :val2}] == Goblin.Tx.select(tx) |> Enum.to_list()

    tx = Goblin.Tx.remove_multi(tx, [:key1, :key2])
    assert nil == Goblin.get(c.db, :key1)
    assert nil == Goblin.get(c.db, :key2)
    assert nil == Goblin.Tx.get(tx, :key1)
    assert nil == Goblin.Tx.get(tx, :key2)
    assert [] == Goblin.Tx.get_multi(tx, [:key1, :key2])
    assert [] == Goblin.Tx.select(tx) |> Enum.to_list()
  end

  test "reads fallback to memtable if not in transaction writes", c do
    Goblin.put(c.db, :key, :val)
    tx = Goblin.Broker.WriteTx.new(@mem_table, @disk_tables)
    assert {:value, {:key, 0, :val}} == Goblin.MemTable.get(@mem_table, :key, 1)
    assert [] == Goblin.DiskTables.search_iterators(@disk_tables, [:key], 1)
    assert :val == Goblin.Tx.get(tx, :key)
    assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key])
    assert [{:key, :val}] == Goblin.Tx.select(tx) |> Enum.to_list()
  end

  @tag db_opts: [mem_limit: 2 * 1024]
  test "reads fallback to disk tables if not in transaction writes", c do
    data = trigger_flush(c.db)
    keys = Enum.map(data, &elem(&1, 0))

    assert_eventually do
      refute Goblin.flushing?(c.db)
    end

    Enum.each(keys, fn key ->
      assert :not_found == Goblin.MemTable.get(@mem_table, key, length(data))
    end)

    assert [_iterator] = Goblin.DiskTables.search_iterators(@disk_tables, keys, length(data))

    tx = Goblin.Broker.WriteTx.new(@mem_table, @disk_tables)

    for {key, val} <- data do
      assert val == Goblin.Tx.get(tx, key)
    end

    assert data == Goblin.Tx.get_multi(tx, keys)
    assert data == Goblin.Tx.select(tx) |> Enum.to_list()
  end
end
