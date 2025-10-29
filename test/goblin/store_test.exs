defmodule Goblin.StoreTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Store
  alias Goblin.SSTs

  @moduletag :tmp_dir
  @db_opts [
    id: :store_test,
    name: :store_test,
    wal_name: :store_test_wal,
    manifest_name: :store_test_manifest,
    sync_interval: 50
  ]

  setup_db(@db_opts)

  test "put/2 puts new SST in store and adds to the compactor", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])

    {:ok,
     %{
       file: file,
       priority: priority,
       key_range: key_range,
       size: size,
       bloom_filter: bf
     } = sst} =
      SSTs.fetch_sst(file)

    assert :ok == Store.put(c.store, sst)

    assert %{ssts: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    assert %{
             levels: %{
               0 => %{
                 entries: %{^file => %{size: ^size, key_range: ^key_range, priority: ^priority}}
               }
             }
           } = :sys.get_state(c.compactor)
  end

  test "put/2 preserves existing files when putting new files", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])

    {:ok,
     %{
       file: file,
       bloom_filter: bf
     } = sst} =
      SSTs.fetch_sst(file)

    assert :ok == Store.put(c.store, sst)
    assert %{ssts: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    assert :ok == Store.put(c.store, sst)

    assert %{
             ssts: [%{file: ^file, bloom_filter: ^bf}, %{file: ^file, bloom_filter: ^bf}]
           } = :sys.get_state(c.store)
  end

  test "remove/2 is idempotent", c do
    assert %{ssts: []} = :sys.get_state(c.store)
    assert :ok == Store.remove(c.store, "foo")
    assert :ok == Store.remove(c.store, "foo")
    assert :ok == Store.remove(c.store, "foo")
    assert %{ssts: []} = :sys.get_state(c.store)
  end

  test "remove/2 removes SST from the store", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])

    {:ok,
     %{
       file: file,
       bloom_filter: bf
     } = sst} =
      SSTs.fetch_sst(file)

    assert :ok == Store.put(c.store, sst)
    assert %{ssts: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)
    assert :ok == Store.remove(c.store, file)
    assert %{ssts: []} = :sys.get_state(c.store)
  end

  test "new_file/1 increments store file counter", c do
    assert %{max_file_count: 0} = :sys.get_state(c.store)
    _ = Store.new_file(c.store)
    assert %{max_file_count: 1} = :sys.get_state(c.store)
    _ = Store.new_file(c.store)
    assert %{max_file_count: 2} = :sys.get_state(c.store)
  end

  test "get/2 returns empty list if no files are stored", c do
    assert [{:k, []}] == Store.get(c.store, :k)
  end

  test "get/2 rlocks file when matched", c do
    self = self()
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])

    {:ok, %{file: file} = sst} = SSTs.fetch_sst(file)

    assert :ok == Store.put(c.store, sst)

    assert [{_, [{read_f, unlock_f}]}] = Store.get(c.store, :k1)

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

  test "get/2 returns empty list if Bloom filter does not match", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, sst} = SSTs.fetch_sst(file)
    assert :ok == Store.put(c.store, sst)
    assert [{:k3, []}] = Store.get(c.store, :k3)
  end

  test "get_iterators/3 returns iterators within key_range", c do
    pid = self()
    assert [] == Store.get_iterators(c.store, nil, nil)

    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, sst} = SSTs.fetch_sst(file)
    assert :ok == Store.put(c.store, sst)

    assert [{{_, _}, unlock_f}] = Store.get_iterators(c.store, nil, nil)

    assert %{locks: %{^file => %{current: [rlock: {^pid, _, _}]}}} = :sys.get_state(c.rw_locks)
    assert :ok == unlock_f.()

    assert %{locks: locks} = :sys.get_state(c.rw_locks)
    assert locks == %{}

    assert [] = Store.get_iterators(c.store, nil, :k0)
    assert [{{_, _}, _}] = Store.get_iterators(c.store, nil, :k2)
    assert [{{_, _}, _}] = Store.get_iterators(c.store, :k1, nil)
    assert [{{_, _}, _}] = Store.get_iterators(c.store, :k0, :k3)
  end

  test "get_iterators/3 returns multiple iterators for multiple SSTs", c do
    file1 = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :a, :v1}, {1, :b, :v2}])
    file2 = write_sst(c.tmp_dir, "bar", 0, 20, [{2, :c, :v3}, {3, :d, :v4}])
    file3 = write_sst(c.tmp_dir, "baz", 0, 30, [{4, :e, :v5}, {5, :f, :v6}])

    {:ok, sst1} = SSTs.fetch_sst(file1)
    {:ok, sst2} = SSTs.fetch_sst(file2)
    {:ok, sst3} = SSTs.fetch_sst(file3)

    assert :ok == Store.put(c.store, sst1)
    assert :ok == Store.put(c.store, sst2)
    assert :ok == Store.put(c.store, sst3)

    iterators = Store.get_iterators(c.store, nil, nil)
    assert length(iterators) == 3

    iterators = Store.get_iterators(c.store, :b, :e)
    assert length(iterators) == 3

    iterators = Store.get_iterators(c.store, :a, :b)
    assert length(iterators) == 1

    iterators = Store.get_iterators(c.store, :g, :z)
    assert length(iterators) == 0
  end

  test "store gets state from manifest on start", c do
    file = write_sst(c.tmp_dir, "foo", 0, 10, [{0, :k1, :v1}, {1, :k2, :v2}])
    {:ok, %{file: file, bloom_filter: bf} = sst} = SSTs.fetch_sst(file)
    assert :ok == Goblin.Manifest.log_compaction(c.manifest, [], [file])
    assert :ok == Store.put(c.store, sst)
    assert %{ssts: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(c.store)

    stop_supervised!(c.db_id)
    %{store: store} = start_db(c.tmp_dir, @db_opts)

    assert %{ssts: [%{file: ^file, bloom_filter: ^bf}]} = :sys.get_state(store)
  end
end
