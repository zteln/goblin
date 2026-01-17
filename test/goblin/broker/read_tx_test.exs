defmodule Goblin.Broker.ReadTxTest do
  use ExUnit.Case, async: true
  use TestHelper

  setup_db(
    mem_limit: 2 * 1024,
    bf_bit_array_size: 1000
  )

  @mem_table __MODULE__.MemTable
  @disk_tables __MODULE__.DiskTables

  describe "get/4" do
    test "can get value associated with key from memtable", c do
      Goblin.put(c.db, :key, :val)

      assert {:key, 0, :val} =
               triple = Goblin.Broker.ReadTx.get(@mem_table, @disk_tables, 1, :key)

      assert {:value, ^triple} = Goblin.MemTable.get(@mem_table, :key, 1)
      assert [] == Goblin.DiskTables.search_iterators(@disk_tables, [:key1], 1)
    end

    @tag db_opts: [mem_limit: 2 * 1024]
    test "can get value associated with key from disk tables", c do
      data = trigger_flush(c.db)

      for {{key, value}, seq} <- Enum.zip(data, 0..length(data)) do
        assert_eventually do
          assert {^key, ^seq, ^value} =
                   triple = Goblin.Broker.ReadTx.get(@mem_table, @disk_tables, seq + 1, key)

          assert [^triple] =
                   Goblin.DiskTables.search_iterators(@disk_tables, [key], seq + 1)
                   |> Goblin.Iterator.k_merge_stream()
                   |> Enum.to_list()

          assert :not_found = Goblin.MemTable.get(@mem_table, key, seq + 1)
        end
      end
    end
  end

  describe "get_multi/4" do
    test "can get values associated with keys from memtable", c do
      Goblin.put(c.db, :key1, :val1)
      Goblin.put(c.db, :key2, :val2)
      Goblin.put(c.db, :key3, :val3)

      assert [
               {:key1, 0, :val1},
               {:key2, 1, :val2},
               {:key3, 2, :val3}
             ] ==
               Goblin.Broker.ReadTx.get_multi(@mem_table, @disk_tables, 3, [:key1, :key2, :key3])

      assert [
               {:value, {:key1, 0, :val1}},
               {:value, {:key2, 1, :val2}},
               {:value, {:key3, 2, :val3}}
             ] == Goblin.MemTable.get_multi(@mem_table, [:key1, :key2, :key3], 3)

      assert [] == Goblin.DiskTables.search_iterators(@disk_tables, [:key1, :key2, :key3], 3)
    end

    test "can get values associated with keys from disk tables", c do
      data = trigger_flush(c.db)

      assert_eventually do
        refute Goblin.flushing?(c.db)
      end

      keys = Enum.map(data, fn {key, _value} -> key end)

      assert Enum.map(data, fn {key, value} -> {key, key - 1, value} end) ==
               Goblin.Broker.ReadTx.get_multi(@mem_table, @disk_tables, length(data), keys)

      assert Enum.map(data, fn {key, value} -> {key, key - 1, value} end) ==
               Goblin.DiskTables.search_iterators(@disk_tables, keys, length(data))
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()

      assert Enum.map(data, fn {key, _value} -> {:not_found, key} end) ==
               Goblin.MemTable.get_multi(@mem_table, keys, length(data))
    end

    test "results from both tables are merged", c do
      data = trigger_flush(c.db)
      Goblin.put(c.db, :key, :val)
      keys = Enum.map(data, fn {key, _value} -> key end)

      assert_eventually do
        assert Enum.map(data, fn {key, _value} -> {:not_found, key} end) ==
                 Goblin.MemTable.get_multi(@mem_table, keys, length(data))
      end

      assert {:value, {:key, length(data), :val}} ==
               Goblin.MemTable.get(@mem_table, :key, length(data) + 1)

      assert ([{:key, length(data), :val}] ++
                Enum.map(data, fn {key, value} -> {key, key - 1, value} end))
             |> List.keysort(0) ==
               Goblin.Broker.ReadTx.get_multi(@mem_table, @disk_tables, length(data) + 1, [
                 :key | keys
               ])
               |> List.keysort(0)
    end
  end

  describe "is transactionable" do
    test "cannot write" do
      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert_raise RuntimeError, fn ->
        Goblin.Tx.put(tx, :key, :val)
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.put_multi(tx, [{:key1, :val1}, {:key2, :val2}])
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.remove(tx, :key)
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.remove_multi(tx, [:key1, :key2])
      end
    end

    test "cannot read writes committed after transaction start", c do
      Goblin.put(c.db, :key1, :val1)
      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)
      assert :val1 == Goblin.Tx.get(tx, :key1)
      assert nil == Goblin.Tx.get(tx, :key2)
      assert [{:key1, :val1}] == Goblin.Tx.get_multi(tx, [:key1, :key2])
      assert [{:key1, :val1}] == Goblin.Tx.select(tx, []) |> Enum.to_list()
      Goblin.put(c.db, :key2, :val2)
      assert :val1 == Goblin.Tx.get(tx, :key1)
      assert nil == Goblin.Tx.get(tx, :key2)
      assert [{:key1, :val1}] == Goblin.Tx.get_multi(tx, [:key1, :key2])
      assert [{:key1, :val1}] == Goblin.Tx.select(tx, []) |> Enum.to_list()
    end

    test "filters tombstones", c do
      Goblin.put(c.db, :key, :val)
      Goblin.remove(c.db, :key)
      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert nil == Goblin.Tx.get(tx, :key)

      trigger_flush(c.db)

      assert_eventually do
        refute Goblin.flushing?(c.db)
      end

      assert nil == Goblin.Tx.get(tx, :key)
    end

    test "can read tagged keys", c do
      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert nil == Goblin.Tx.get(tx, :key)
      assert nil == Goblin.Tx.get(tx, :key, tag: :a_tag)

      Goblin.put(c.db, :key, :val, tag: :a_tag)

      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert nil == Goblin.Tx.get(tx, :key)
      assert :val == Goblin.Tx.get(tx, :key, tag: :a_tag)
      assert nil == Goblin.Tx.get(tx, :key, tag: :another_tag)

      assert [] == Goblin.Tx.get_multi(tx, [:key])
      assert [{:key, :val}] == Goblin.Tx.get_multi(tx, [:key], tag: :a_tag)
      assert [] == Goblin.Tx.get_multi(tx, [:key], tag: :another_tag)

      assert [] == Goblin.Tx.select(tx) |> Enum.to_list()
      assert [{:a_tag, :key, :val}] == Goblin.Tx.select(tx, tag: :a_tag) |> Enum.to_list()
      assert [{:a_tag, :key, :val}] == Goblin.Tx.select(tx, tag: :all) |> Enum.to_list()
      assert [] == Goblin.Tx.select(tx, tag: :another_tag) |> Enum.to_list()
    end
  end
end
