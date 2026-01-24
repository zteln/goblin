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
      key1 = :key1
      key2 = :key2
      val = :val

      Goblin.put(c.db, key1, val)

      assert {key1, 0, val} ==
               Goblin.Broker.ReadTx.get(@mem_table, @disk_tables, 1, key1)

      assert [{:value, {key1, 0, val}}] == Goblin.MemTable.get_multi(@mem_table, [key1], 1)
      assert [] == Goblin.DiskTables.search_iterators(@disk_tables, [key2], 1)
    end

    @tag db_opts: [mem_limit: 2 * 1024]
    test "can get value associated with key from disk tables", c do
      data = trigger_flush(c.db, c.tmp_dir)

      for {{key, value}, seq} <- Enum.zip(data, 0..length(data)) do
        assert_eventually do
          assert {^key, ^seq, ^value} =
                   triple = Goblin.Broker.ReadTx.get(@mem_table, @disk_tables, seq + 1, key)

          assert [^triple] =
                   Goblin.DiskTables.search_iterators(@disk_tables, [key], seq + 1)
                   |> Goblin.Iterator.k_merge_stream()
                   |> Enum.to_list()

          assert [{:not_found, key}] == Goblin.MemTable.get_multi(@mem_table, [key], seq + 1)
        end
      end
    end
  end

  describe "get_multi/4" do
    test "can get values associated with keys from memtable", c do
      # can handle keys with same value but different types
      key1 = 0
      key2 = 0.0
      key3 = :key3
      val1 = :val1
      val2 = :val2
      val3 = :val3

      Goblin.put(c.db, key1, val1)
      Goblin.put(c.db, key2, val2)
      Goblin.put(c.db, key3, val3)

      assert [
               {key1, 0, val1},
               {key2, 1, val2},
               {key3, 2, val3}
             ]
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> TestHelper.uniq_by_value(fn {key, _seq, _val} -> key end) ==
               Goblin.Broker.ReadTx.get_multi(@mem_table, @disk_tables, 3, [key1, key2, key3])

      assert [
               {key1, 0, val1},
               {key2, 1, val2},
               {key3, 2, val3}
             ]
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> TestHelper.uniq_by_value(fn {key, _seq, _val} -> key end)
             |> Enum.map(&{:value, &1}) ==
               Goblin.MemTable.get_multi(
                 @mem_table,
                 Enum.sort([key1, key2, key3]) |> TestHelper.uniq_by_value(),
                 3
               )

      assert [] == Goblin.DiskTables.search_iterators(@disk_tables, [key1, key2, key3], 3)
    end

    test "can get values associated with keys from disk tables", c do
      pairs = trigger_flush(c.db, c.tmp_dir)

      triples =
        pairs
        |> Enum.with_index(fn {key, value}, idx -> {key, idx, value} end)
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> TestHelper.uniq_by_value(fn {key, _seq, _val} -> key end)

      keys = Enum.map(pairs, &elem(&1, 0))

      assert triples ==
               Goblin.Broker.ReadTx.get_multi(@mem_table, @disk_tables, length(pairs), keys)

      assert triples ==
               Goblin.DiskTables.search_iterators(@disk_tables, keys, length(pairs))
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()

      assert Enum.map(keys, fn key -> {:not_found, key} end) ==
               Goblin.MemTable.get_multi(@mem_table, keys, length(pairs))
    end

    test "results from both tables are merged", c do
      key = :key
      val = :val

      pairs = trigger_flush(c.db, c.tmp_dir)

      triples =
        pairs
        |> Enum.with_index(fn {key, value}, idx -> {key, idx, value} end)

      Goblin.put(c.db, key, val)
      keys = Enum.map(pairs, &elem(&1, 0))

      assert_eventually do
        assert Enum.map(keys, &{:not_found, &1}) ==
                 Goblin.MemTable.get_multi(@mem_table, keys, length(pairs))
      end

      assert [{:value, {key, length(pairs), val}}] ==
               Goblin.MemTable.get_multi(@mem_table, [key], length(pairs) + 1)

      assert ([{key, length(pairs), val}] ++ triples)
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> TestHelper.uniq_by_value(fn {key, _seq, _val} -> key end) ==
               Goblin.Broker.ReadTx.get_multi(
                 @mem_table,
                 @disk_tables,
                 length(pairs) + 1,
                 [
                   key | keys
                 ]
               )
               |> List.keysort(0)
    end
  end

  describe "is transactionable" do
    test "cannot write" do
      [key1, key2, val1, val2] = StreamData.term() |> Enum.take(4)
      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert_raise RuntimeError, fn ->
        Goblin.Tx.put(tx, key1, val1)
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.put_multi(tx, [{key1, val1}, {key2, val2}])
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.remove(tx, key1)
      end

      assert_raise RuntimeError, fn ->
        Goblin.Tx.remove_multi(tx, [key1, key2])
      end
    end

    test "cannot read writes committed after transaction start", c do
      key1 = :key1
      key2 = :key2
      val1 = :val1
      val2 = :val2

      Goblin.put(c.db, key1, val1)
      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert val1 == Goblin.Tx.get(tx, key1)
      assert nil == Goblin.Tx.get(tx, key2)
      assert [{key1, val1}] == Goblin.Tx.get_multi(tx, [key1, key2])
      assert [{key1, val1}] == Goblin.Tx.select(tx, []) |> Enum.to_list()

      Goblin.put(c.db, key2, val2)

      assert val1 == Goblin.Tx.get(tx, key1)
      assert nil == Goblin.Tx.get(tx, key2)
      assert [{key1, val1}] == Goblin.Tx.get_multi(tx, [key1, key2])
      assert [{key1, val1}] == Goblin.Tx.select(tx, []) |> Enum.to_list()
    end

    test "filters tombstones", c do
      key = :key
      val = :val

      Goblin.put(c.db, key, val)
      Goblin.remove(c.db, key)

      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert nil == Goblin.Tx.get(tx, key)

      trigger_flush(c.db, c.tmp_dir)

      assert_eventually do
        refute Goblin.flushing?(c.db)
      end

      assert nil == Goblin.Tx.get(tx, key)
    end

    test "can read tagged keys", c do
      key = :key
      val = :val
      tag1 = :tag1
      tag2 = :tag2

      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert nil == Goblin.Tx.get(tx, key)
      assert nil == Goblin.Tx.get(tx, key, tag: tag1)

      Goblin.put(c.db, key, val, tag: tag1)

      tx = Goblin.Broker.ReadTx.new(@mem_table, @disk_tables)

      assert nil == Goblin.Tx.get(tx, key)
      assert val == Goblin.Tx.get(tx, key, tag: tag1)
      assert nil == Goblin.Tx.get(tx, key, tag: tag2)

      assert [] == Goblin.Tx.get_multi(tx, [key])
      assert [{key, val}] == Goblin.Tx.get_multi(tx, [key], tag: tag1)
      assert [] == Goblin.Tx.get_multi(tx, [key], tag: tag2)

      assert [] == Goblin.Tx.select(tx) |> Enum.to_list()
      assert [{tag1, key, val}] == Goblin.Tx.select(tx, tag: tag1) |> Enum.to_list()
      assert [{tag1, key, val}] == Goblin.Tx.select(tx, tag: :all) |> Enum.to_list()
      assert [] == Goblin.Tx.select(tx, tag: tag2) |> Enum.to_list()
    end
  end
end
