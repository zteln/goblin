defmodule Goblin.Broker.WriteTxTest do
  use ExUnit.Case, async: true
  use TestHelper

  setup_db(
    mem_limit: 2 * 1024,
    bf_bit_array_size: 1000
  )

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
    assert [{:value, {:key, 0, :val}}] == Goblin.MemTable.get_multi(@mem_table, [:key], 1)
    assert [] == Goblin.DiskTables.search_iterators(@disk_tables, [:key], 1)
    assert :val == Goblin.Tx.get(tx, :key)
    assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key])
    assert [{:key, :val}] == Goblin.Tx.select(tx) |> Enum.to_list()
  end

  test "reads fallback to disk tables if not in transaction writes", c do
    triples =
      trigger_flush(c.db, c.tmp_dir)
      |> Enum.with_index(fn {key, val}, seq -> {key, seq, val} end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
      |> TestHelper.uniq_by_value(&elem(&1, 0))

    keys = Enum.map(triples, &elem(&1, 0))

    assert_eventually do
      refute Goblin.flushing?(c.db)
    end

    Enum.each(keys, fn key ->
      assert [{:not_found, key}] == Goblin.MemTable.get_multi(@mem_table, [key], length(triples))
    end)

    assert [_iterator] = Goblin.DiskTables.search_iterators(@disk_tables, keys, length(triples))

    tx = Goblin.Broker.WriteTx.new(@mem_table, @disk_tables)

    triples
    |> Enum.each(fn {key, _seq, val} ->
      assert val == Goblin.Tx.get(tx, key)
    end)

    assert Enum.map(triples, fn {key, _seq, val} -> {key, val} end) ==
             Goblin.Tx.get_multi(tx, keys)

    assert Enum.map(triples, fn {key, _seq, val} -> {key, val} end) ==
             Goblin.Tx.select(tx) |> Enum.to_list()
  end

  test "can write tagged keys", c do
    Goblin.put(c.db, :key1, :val1, tag: :a_separate_tag)

    tx = Goblin.Broker.WriteTx.new(@mem_table, @disk_tables)
    tx = Goblin.Tx.put(tx, :key, :val, tag: :a_tag)

    assert nil == Goblin.Tx.get(tx, :key)
    assert :val == Goblin.Tx.get(tx, :key, tag: :a_tag)
    assert nil == Goblin.Tx.get(tx, :key, tag: :another_tag)

    assert [] == Goblin.Tx.get_multi(tx, [:key])
    assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key], tag: :a_tag)
    assert [{:key1, :val1}] == Goblin.Tx.get_multi(tx, [:key, :key1], tag: :a_separate_tag)
    assert [] == Goblin.Tx.get_multi(tx, [:key], tag: :another_tag)

    assert [] == Goblin.Tx.select(tx) |> Enum.to_list()
    assert [{:a_tag, :key, :val}] == Goblin.Tx.select(tx, tag: :a_tag) |> Enum.to_list()
    assert [] == Goblin.Tx.select(tx, tag: :another_tag) |> Enum.to_list()

    assert [{:a_tag, :key, :val}, {:a_separate_tag, :key1, :val1}] |> Enum.sort_by(&elem(&1, 1)) ==
             Goblin.Tx.select(tx, tag: :all) |> Enum.to_list() |> Enum.sort_by(&elem(&1, 1))
  end
end
