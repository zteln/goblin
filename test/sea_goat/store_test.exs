defmodule SeaGoat.StoreTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoat.Store
  alias SeaGoat.Writer
  alias SeaGoat.SSTables

  @moduletag :tmp_dir

  setup_db(
    id: :store_test_id,
    name: :store_test,
    wal_name: :store_test_wal,
    sync_interval: 50
  )

  test "put/4 puts new SSTable in store", c do
    file = Store.new_file(c.store)
    assert String.ends_with?(file, "1.seagoat")
    level = 0

    mem_table =
      for n <- 1..10, reduce: Writer.MemTable.new() do
        acc ->
          Writer.MemTable.upsert(acc, n, "v-#{n}")
      end

    {:ok, bf, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file, level)

    assert :ok == Store.put(c.store, file, bf, level)

    assert %{levels: %{0 => [{^file, ^bf}]}} = :sys.get_state(c.store)
  end

  test "put/4 handles multiple files at same level", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)
    level = 0

    mem_table1 =
      for n <- 1..5, reduce: Writer.MemTable.new() do
        acc -> Writer.MemTable.upsert(acc, n, "v1-#{n}")
      end

    {:ok, bf1, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table1, file1, level)

    mem_table2 =
      for n <- 6..10, reduce: Writer.MemTable.new() do
        acc -> Writer.MemTable.upsert(acc, n, "v2-#{n}")
      end

    {:ok, bf2, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table2, file2, level)

    assert :ok == Store.put(c.store, file1, bf1, level)
    assert :ok == Store.put(c.store, file2, bf2, level)

    state = :sys.get_state(c.store)
    assert [{^file2, ^bf2}, {^file1, ^bf1}] = state.levels[0]
  end

  test "put/4 handles files at different levels", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)
    file3 = Store.new_file(c.store)

    mem_table =
      for n <- 1..3, reduce: Writer.MemTable.new() do
        acc -> Writer.MemTable.upsert(acc, n, "v-#{n}")
      end

    {:ok, bf1, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file1, 0)

    {:ok, bf2, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file2, 1)

    {:ok, bf3, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file3, 2)

    assert :ok == Store.put(c.store, file1, bf1, 0)
    assert :ok == Store.put(c.store, file2, bf2, 1)
    assert :ok == Store.put(c.store, file3, bf3, 2)

    state = :sys.get_state(c.store)
    assert [{file1, bf1}] == state.levels[0]
    assert [{file2, bf2}] == state.levels[1]
    assert [{file3, bf3}] == state.levels[2]
  end

  test "put/4 notifies compactor about new file", c do
    file = Store.new_file(c.store)
    level = 1

    mem_table =
      for n <- 1..5, reduce: Writer.MemTable.new() do
        acc -> Writer.MemTable.upsert(acc, n, "v-#{n}")
      end

    {:ok, bf, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file, level)

    assert :ok == Store.put(c.store, file, bf, level)

    assert_eventually do
      assert %{levels: %{1 => [{1, ^file}]}} = :sys.get_state(c.compactor)
    end
  end

  test "put/4 preserves existing files when adding new ones", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)
    level = 0

    mem_table1 = Writer.MemTable.new() |> Writer.MemTable.upsert(1, "value1")
    mem_table2 = Writer.MemTable.new() |> Writer.MemTable.upsert(2, "value2")

    {:ok, bf1, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table1, file1, level)

    {:ok, bf2, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table2, file2, level)

    assert :ok == Store.put(c.store, file1, bf1, level)

    state1 = :sys.get_state(c.store)
    assert [{file1, bf1}] == state1.levels[0]

    assert :ok == Store.put(c.store, file2, bf2, level)

    state2 = :sys.get_state(c.store)
    level_files = state2.levels[0]
    assert length(level_files) == 2
    assert {file1, bf1} in level_files
    assert {file2, bf2} in level_files
  end

  test "remove/3 removes SSTable from level", c do
    file = Store.new_file(c.store)
    level = 0

    state = :sys.get_state(c.store)
    assert %{} == state.levels

    assert :ok == Store.remove(c.store, [file], 0)

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert(1, "value1")

    {:ok, bf, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file, level)

    assert :ok == Store.put(c.store, file, bf, level)

    state = :sys.get_state(c.store)
    assert %{0 => [{^file, _bf}]} = state.levels

    assert :ok == Store.remove(c.store, [file], 0)

    state = :sys.get_state(c.store)
    assert %{} == state.levels
  end

  test "remove/3 handles removing from non-existent level", c do
    assert :ok == Store.remove(c.store, ["non_existent_file.seagoat"], 5)

    state = :sys.get_state(c.store)
    assert %{} == state.levels
  end

  test "remove/3 handles removing non-existent files from existing level", c do
    file = Store.new_file(c.store)
    level = 0

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert(1, "value1")

    {:ok, bf, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file, level)

    assert :ok == Store.put(c.store, file, bf, level)

    assert :ok == Store.remove(c.store, ["non_existent.seagoat"], level)

    state = :sys.get_state(c.store)
    assert %{0 => [{^file, ^bf}]} = state.levels
  end

  test "remove/3 removes multiple files from level", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)
    file3 = Store.new_file(c.store)
    level = 0

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert(1, "value1")

    {:ok, bf1, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file1, level)

    {:ok, bf2, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file2, level)

    {:ok, bf3, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file3, level)

    assert :ok == Store.put(c.store, file1, bf1, level)
    assert :ok == Store.put(c.store, file2, bf2, level)
    assert :ok == Store.put(c.store, file3, bf3, level)

    assert :ok == Store.remove(c.store, [file1, file3], level)

    state = :sys.get_state(c.store)
    assert %{0 => [{^file2, ^bf2}]} = state.levels
  end

  test "new_file/1 generates sequential filenames", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)
    file3 = Store.new_file(c.store)

    assert String.ends_with?(file1, "1.seagoat")
    assert String.ends_with?(file2, "2.seagoat")
    assert String.ends_with?(file3, "3.seagoat")

    assert file1 != file2
    assert file2 != file3
    assert file1 != file3
  end

  test "new_file/1 uses correct directory", c do
    file = Store.new_file(c.store)

    assert String.starts_with?(file, c.tmp_dir)
    assert String.ends_with?(file, ".seagoat")
  end

  test "reuse_file/2 returns and removes file from compacting_files", c do
    files_key = ["file1.seagoat", "file2.seagoat"]
    reused_file = "reused_file.seagoat"

    :sys.replace_state(c.store, fn state ->
      compacting_files = Map.put(state.compacting_files, files_key, reused_file)
      %{state | compacting_files: compacting_files}
    end)

    result = Store.reuse_file(c.store, files_key)
    assert result == reused_file

    state = :sys.get_state(c.store)
    assert not Map.has_key?(state.compacting_files, files_key)
  end

  test "reuse_file/2 returns nil for non-existent files", c do
    non_existent_files = ["non_existent1.seagoat", "non_existent2.seagoat"]

    result = Store.reuse_file(c.store, non_existent_files)
    assert result == nil
  end

  test "get_ss_tables/2 returns empty list when no matching bloom filters", c do
    file = Store.new_file(c.store)
    level = 0

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert("existing_key", "value")

    {:ok, bf, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file, level)

    assert :ok == Store.put(c.store, file, bf, level)

    result = Store.get_ss_tables(c.store, "non_existent_key")
    assert result == []
  end

  test "get_ss_tables/2 returns SSTables for potentially matching keys", c do
    file = Store.new_file(c.store)
    level = 0
    key = "test_key"

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert(key, "value")

    {:ok, bf, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file, level)

    assert :ok == Store.put(c.store, file, bf, level)

    result = Store.get_ss_tables(c.store, key)

    assert is_list(result)
    assert length(result) == 1

    [{read_fn, unlock_fn}] = result
    assert is_function(read_fn, 0)
    assert is_function(unlock_fn, 0)

    unlock_fn.()
  end

  test "get_ss_tables/2 searches across multiple levels", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)
    key = "shared_key"

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert(key, "value")

    {:ok, bf1, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file1, 0)

    {:ok, bf2, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file2, 1)

    assert :ok == Store.put(c.store, file1, bf1, 0)
    assert :ok == Store.put(c.store, file2, bf2, 1)

    result = Store.get_ss_tables(c.store, key)

    assert length(result) == 2

    Enum.each(result, fn {_read_fn, unlock_fn} -> unlock_fn.() end)
  end

  test "tmp_file/1 adds .tmp suffix" do
    assert Store.tmp_file("test.seagoat") == "test.seagoat.tmp"
    assert Store.tmp_file("/path/to/file.seagoat") == "/path/to/file.seagoat.tmp"
    assert Store.tmp_file("simple") == "simple.tmp"
  end

  test "dump_file/1 adds .dump suffix" do
    assert Store.dump_file("test.seagoat") == "test.seagoat.dump"
    assert Store.dump_file("/path/to/file.seagoat") == "/path/to/file.seagoat.dump"
    assert Store.dump_file("simple") == "simple.dump"
  end

  test "store gets same state after reboot", c do
    file1 = Store.new_file(c.store)
    file2 = Store.new_file(c.store)

    mem_table = Writer.MemTable.new() |> Writer.MemTable.upsert("key", "value")

    {:ok, bf1, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file1, 0)

    {:ok, bf2, _file, _level} =
      SSTables.write(%SSTables.MemTableIterator{}, mem_table, file2, 1)

    assert :ok == Store.put(c.store, file1, bf1, 0)
    assert :ok == Store.put(c.store, file2, bf2, 1)

    %{levels: levels} = :sys.get_state(c.store)

    stop_supervised!(c.db_id)

    %{store: store} =
      start_db(c.tmp_dir,
        id: :store_test_id,
        name: :store_test,
        wal_name: :store_test_wal,
        sync_interval: 50
      )

    assert %{levels: ^levels} = :sys.get_state(store)
  end
end
