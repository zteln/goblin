defmodule SeaGoatDB.StoreTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoatDB.Store
  alias SeaGoatDB.SSTs

  @moduletag :tmp_dir
  @db_opts [
    id: :store_test,
    name: :store_test,
    wal_name: :store_test_wal,
    manifest_name: :store_test_manifest,
    sync_interval: 50
  ]

  setup_db(@db_opts)

  test "put/4 puts new SST in store and adds to the compactor", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, bf, level_key, priority, size, key_range} = SSTs.fetch_sst_info(file)
    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)

    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    assert %{
             levels: %{
               0 => %{
                 entries: %{^file => %{size: ^size, key_range: ^key_range, priority: ^priority}}
               }
             }
           } = :sys.get_state(c.compactor)
  end

  test "put/4 preserves existing files when putting new files", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, bf, level_key, priority, size, key_range} = SSTs.fetch_sst_info(file)
    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)
    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)

    assert %{
             ss_tables: [%{file: ^file, bloom_filter: ^bf}, %{file: ^file, bloom_filter: ^bf}]
           } = :sys.get_state(c.store)
  end

  test "remove/2 is idempotent", c do
    assert %{ss_tables: []} = :sys.get_state(c.store)
    assert :ok == Store.remove(c.store, "foo")
    assert :ok == Store.remove(c.store, "foo")
    assert :ok == Store.remove(c.store, "foo")
    assert %{ss_tables: []} = :sys.get_state(c.store)
  end

  test "remove/2 removes SST from the store", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, bf, level_key, priority, size, key_range} = SSTs.fetch_sst_info(file)

    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)
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
    assert [] == Store.get(c.store, :k)
  end

  test "get/2 rlocks file when matched", c do
    self = self()
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, bf, level_key, priority, size, key_range} = SSTs.fetch_sst_info(file)

    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)

    assert [{read_f, unlock_f}] = Store.get(c.store, :k1)

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
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, bf, level_key, priority, size, key_range} = SSTs.fetch_sst_info(file)

    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)

    assert [] = Store.get(c.store, :k3)
  end

  test "store gets state from manifest on start", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, bf, level_key, priority, size, key_range} = SSTs.fetch_sst_info(file)

    assert :ok == SeaGoatDB.Manifest.log_compaction(c.manifest, [], [file])
    assert :ok == Store.put(c.store, file, level_key, bf, priority, size, key_range)
    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    stop_supervised!(c.db_id)
    %{store: store} = start_db(c.tmp_dir, @db_opts)

    assert %{ss_tables: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(store)
  end
end
