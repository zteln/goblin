defmodule SeaGoat.StoreTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoat.Store

  @moduletag :tmp_dir
  @db_opts [
    id: :store_test,
    name: :store_test,
    wal_name: :store_test_wal,
    manifest_name: :store_test_manifest,
    sync_interval: 50
  ]

  setup_db(@db_opts)

  test "put/4 puts new SSTable in store and adds to the compactor", c do
    file = Path.join(c.tmp_dir, "foo.seagoat")

    {:ok, bf, level_key, size, key_range} =
      SeaGoat.SSTables.write(file, 0, [{0, :k1, :v1}, {1, :k2, :v2}])

    assert :ok == Store.put(c.store, file, level_key, {bf, 0, size, key_range})

    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    assert %{
             levels: %{
               0 => %{entries: %{^file => %{size: ^size, key_range: ^key_range, priority: 0}}}
             }
           } = :sys.get_state(c.compactor)
  end

  test "put/4 preserves existing files when putting new files", c do
    file1 = Path.join(c.tmp_dir, "foo.seagoat")
    file2 = Path.join(c.tmp_dir, "foo.seagoat")

    {:ok, bf1, level_key1, size1, key_range1} =
      SeaGoat.SSTables.write(file1, 0, [{0, :k1, :v1}, {1, :k2, :v2}])

    {:ok, bf2, level_key2, size2, key_range2} =
      SeaGoat.SSTables.write(file2, 0, [{0, :k2, :v2}, {2, :k2, :v2}])

    assert :ok == Store.put(c.store, file1, level_key1, {bf1, 0, size1, key_range1})

    assert %{ss_tables: [%{file: ^file1, bloom_filter: ^bf1}]} = :sys.get_state(c.store)

    assert :ok == Store.put(c.store, file2, level_key2, {bf2, 0, size2, key_range2})

    assert %{
             ss_tables: [%{file: ^file2, bloom_filter: ^bf2}, %{file: ^file1, bloom_filter: ^bf1}]
           } = :sys.get_state(c.store)
  end

  test "remove/2 is idempotent", c do
    assert %{ss_tables: []} = :sys.get_state(c.store)
    assert :ok == Store.remove(c.store, "foo")
    assert :ok == Store.remove(c.store, "foo")
    assert :ok == Store.remove(c.store, "foo")
    assert %{ss_tables: []} = :sys.get_state(c.store)
  end

  test "remove/2 removes SSTable from the store", c do
    file = Path.join(c.tmp_dir, "foo.seagoat")

    {:ok, bf, level_key, size, key_range} =
      SeaGoat.SSTables.write(file, 0, [{0, :k1, :v1}, {1, :k2, :v2}])

    assert :ok == Store.put(c.store, file, level_key, {bf, 0, size, key_range})
    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)
    assert :ok == Store.remove(c.store, file)
    assert %{ss_tables: []} = :sys.get_state(c.store)
  end

  test "new_file/1 increments store file counter", c do
    assert %{max_file_count: 0} = :sys.get_state(c.store)
    _ = Store.new_file(c.store)
    assert %{max_file_count: 1} = :sys.get_state(c.store)
    _ = Store.new_file(c.store)
    assert %{max_file_count: 2} = :sys.get_state(c.store)
  end

  test "get_ss_tables/2 returns empty list if no files are stored", c do
    assert [] == Store.get_ss_tables(c.store, :k)
  end

  test "get_ss_tables/2 rlocks file when matched", c do
    self = self()
    file = Path.join(c.tmp_dir, "foo.seagoat")

    {:ok, bf, level_key, size, key_range} =
      SeaGoat.SSTables.write(file, 0, [{0, :k1, :v1}, {1, :k2, :v2}])

    assert :ok == Store.put(c.store, file, level_key, {bf, 0, size, key_range})

    assert [{read_f, unlock_f}] = Store.get_ss_tables(c.store, :k1)

    assert %{
             locks: %{
               ^file => %{
                 current: [
                   {:rlock, {^self, _, _}}
                 ]
               }
             }
           } = :sys.get_state(c.rw_locks)

    assert {:ok, {:value, 0, :v1}} == read_f.()
    assert :ok == unlock_f.()

    assert %{
             locks: locks
           } = :sys.get_state(c.rw_locks)

    assert %{} == locks
  end

  test "get_ss_tables/2 returns empty list if Bloom filter does not match", c do
    file = Path.join(c.tmp_dir, "foo.seagoat")

    {:ok, bf, level_key, size, key_range} =
      SeaGoat.SSTables.write(file, 0, [{0, :k1, :v1}, {1, :k2, :v2}])

    assert :ok == Store.put(c.store, file, level_key, {bf, 0, size, key_range})

    assert [] = Store.get_ss_tables(c.store, :k3)
  end

  test "tmp_file/1 adds .tmp suffix" do
    assert Store.tmp_file("test.seagoat") == "test.seagoat.tmp"
    assert Store.tmp_file("/path/to/file.seagoat") == "/path/to/file.seagoat.tmp"
    assert Store.tmp_file("simple") == "simple.tmp"
  end

  test "store gets state from manifest on start", c do
    file = Path.join(c.tmp_dir, "foo.seagoat")

    {:ok, bf, level_key, size, key_range} =
      SeaGoat.SSTables.write(file, 0, [{0, :k1, :v1}, {1, :k2, :v2}])

    assert :ok == SeaGoat.Manifest.log_compaction(c.manifest, [], [file])
    assert :ok == Store.put(c.store, file, level_key, {bf, 0, size, key_range})
    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    stop_supervised!(c.db_id)
    %{store: store} = start_db(c.tmp_dir, @db_opts)

    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(store)
  end
end
