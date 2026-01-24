defmodule Goblin.MemTableTest do
  use ExUnit.Case, async: true
  use TestHelper
  use Mimic
  import ExUnit.CaptureLog
  @mem_table __MODULE__.MemTable
  @disk_tables __MODULE__.DiskTables
  setup_db(
    mem_limit: 2 * 1024,
    bf_bit_array_size: 1000
  )

  test "can read and write data", c do
    assert :not_found == Goblin.MemTable.get(@mem_table, :key, 0)
    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 0, :key, :val}], 1)
    assert {:value, {:key, 0, :val}} == Goblin.MemTable.get(@mem_table, :key, 1)
    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 1, :key, :new_val}], 2)
    assert {:value, {:key, 1, :new_val}} == Goblin.MemTable.get(@mem_table, :key, 2)
    assert {:value, {:key, 0, :val}} == Goblin.MemTable.get(@mem_table, :key, 1)
    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 2, :another_key, :another_val}], 3)

    assert [
             {:value, {:key, 1, :new_val}},
             {:value, {:another_key, 2, :another_val}}
           ] == Goblin.MemTable.get_multi(@mem_table, [:key, :another_key], 3)

    assert [
             {:value, {:key, 1, :new_val}},
             {:not_found, :non_existing_key}
           ] == Goblin.MemTable.get_multi(@mem_table, [:key, :non_existing_key], 3)
  end

  test "can iterate through data", c do
    iterator = Goblin.MemTable.iterator(@mem_table, 0)
    assert [] == Goblin.Iterator.k_merge_stream([iterator]) |> Enum.to_list()

    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 0, :key3, :val3}], 1)
    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 1, :key1, :val1}], 2)
    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 2, :key2, :val2}], 3)

    assert [] == Goblin.Iterator.k_merge_stream([iterator]) |> Enum.to_list()

    assert 3 == Goblin.MemTable.commit_seq(@mem_table)

    iterator = Goblin.MemTable.iterator(@mem_table, 3)

    assert [
             {:key1, 1, :val1},
             {:key2, 2, :val2},
             {:key3, 0, :val3}
           ] == Goblin.Iterator.k_merge_stream([iterator]) |> Enum.to_list()
  end

  test "recovers state on start", c do
    assert :ok == Goblin.MemTable.insert(c.mem_table, [{:put, 0, :key, :val}], 1)
    assert {:value, {:key, 0, :val}} == Goblin.MemTable.get(@mem_table, :key, 1)

    stop_db(__MODULE__)
    start_db(c.tmp_dir, name: __MODULE__)

    assert {:value, {:key, 0, :val}} == Goblin.MemTable.get(@mem_table, :key, 1)
  end

  test "memory is flushed to disk when exceeding memory limit", c do
    data = trigger_flush(c.db)
    {min_key, _} = Enum.min_by(data, &elem(&1, 0))
    {max_key, _} = Enum.max_by(data, &elem(&1, 0))

    assert_eventually do
      refute Goblin.MemTable.flushing?(c.mem_table)
    end

    assert data ==
             Goblin.DiskTables.stream_iterators(
               __MODULE__.DiskTables,
               min_key,
               max_key,
               length(data)
             )
             |> Goblin.Iterator.k_merge_stream()
             |> Enum.map(fn {k, _s, v} -> {k, v} end)
  end

  test "all versions of keys are flushed", c do
    Goblin.put(c.db, :key1, :val1_1)
    Goblin.put(c.db, :key1, :val1_2)
    Goblin.put(c.db, :key1, :val1_3)
    Goblin.put(c.db, :key2, :val2_1)
    assert {:value, {:key1, 0, :val1_1}} == Goblin.MemTable.get(@mem_table, :key1, 1)
    assert {:value, {:key1, 1, :val1_2}} == Goblin.MemTable.get(@mem_table, :key1, 2)
    assert {:value, {:key1, 2, :val1_3}} == Goblin.MemTable.get(@mem_table, :key1, 3)
    assert {:value, {:key2, 3, :val2_1}} == Goblin.MemTable.get(@mem_table, :key2, 4)

    trigger_flush(c.db)

    assert_eventually do
      assert [{:key1, 0, :val1_1}] ==
               Goblin.DiskTables.search_iterators(@disk_tables, [:key1], 1)
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()

      assert [{:key1, 1, :val1_2}] ==
               Goblin.DiskTables.search_iterators(@disk_tables, [:key1], 2)
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()

      assert [{:key1, 2, :val1_3}] ==
               Goblin.DiskTables.search_iterators(@disk_tables, [:key1], 3)
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()

      assert [{:key2, 3, :val2_1}] ==
               Goblin.DiskTables.search_iterators(@disk_tables, [:key2], 4)
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()
    end
  end

  test "server is stopped if flush fails", %{mem_table: mem_table} = c do
    Process.monitor(mem_table)
    Process.flag(:trap_exit, true)

    Goblin.DiskTables
    |> expect(:new, fn _server, _stream, _opts ->
      {:error, :flush_failed}
    end)

    Goblin.DiskTables
    |> allow(self(), mem_table)

    {_result, _log} =
      with_log(fn ->
        trigger_flush(c.db)
        assert_receive {:DOWN, _ref, :process, ^mem_table, :flush_failed}
      end)
  end

  test "mem table is only cleaned when there are no streamers", c do
    Goblin.MemTable.Store.inc_streamers(@mem_table)
    trigger_flush(c.db, c.tmp_dir)
    trigger_flush(c.db, c.tmp_dir)
    trigger_flush(c.db, c.tmp_dir)

    assert_eventually do
      refute Goblin.flushing?(c.db)
    end

    size = Goblin.MemTable.Store.size(@mem_table)

    assert Goblin.MemTable.Store.size(@mem_table) == size

    Goblin.MemTable.Store.deinc_streamers(@mem_table)

    assert_eventually do
      assert Goblin.MemTable.Store.size(@mem_table) < size
    end
  end
end
