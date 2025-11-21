defmodule Goblin.StoreTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Store

  @moduletag :tmp_dir
  setup_db()

  describe "on start" do
    test "starts with empty state", c do
      assert %{max_file_count: 0, local_name: __MODULE__.Store} = :sys.get_state(c.store)
    end

    test "recovers state from manifest", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst(file, [{1, 0, "v-1"}, {2, 1, "v-2"}])

      Goblin.Manifest.log_flush(c.manifest, [sst.file], "bar")

      stop_db(__MODULE__)
      %{store: store} = start_db(c.tmp_dir, name: __MODULE__)

      assert %{max_file_count: 1} = :sys.get_state(store)
    end
  end

  describe "put/2, remove/2" do
    test "updates store", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst(file, [{1, 0, "v-1"}])

      assert :ok == Store.put(c.store, [sst])

      assert {1, [^sst]} = Store.get(__MODULE__.Store, 1)

      assert :ok == Store.remove(c.store, [file])

      assert {1, []} == Store.get(__MODULE__.Store, 1)
    end

    test "can insert same file multiple times", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst(file, [{1, 0, "v-1"}])

      assert :ok == Store.put(c.store, [sst])
      assert :ok == Store.put(c.store, [sst])
      assert :ok == Store.put(c.store, [sst])
      assert {1, [^sst]} = Store.get(__MODULE__.Store, 1)
    end

    test "can remove multiple times", c do
      assert :ok == Store.remove(c.store, ["foo"])
      assert :ok == Store.remove(c.store, ["foo"])
      assert :ok == Store.remove(c.store, ["foo"])
      assert {1, []} == Store.get(__MODULE__.Store, 1)
    end
  end

  describe "new_file/1" do
    test "increments for every call", c do
      file = Store.new_file(c.store)
      assert String.ends_with?(file, "0.goblin")
      file = Store.new_file(c.store)
      assert String.ends_with?(file, "1.goblin")
      file = Store.new_file(c.store)
      assert String.ends_with?(file, "2.goblin")
    end
  end

  describe "get/2, iterators/3" do
    test "returns SST for a single key", c do
      file1 = Path.join(c.tmp_dir, "foo")
      sst1 = fake_sst(file1, [{1, 0, "v-1"}, {2, 1, "v-2"}])
      file2 = Path.join(c.tmp_dir, "bar")
      sst2 = fake_sst(file2, [{3, 2, "v-3"}, {4, 3, "v-4"}])
      file3 = Path.join(c.tmp_dir, "baz")
      sst3 = fake_sst(file3, [{2, 4, "w-2"}, {3, 5, "w-3"}])
      Store.put(c.store, [sst1])
      Store.put(c.store, [sst2])
      Store.put(c.store, [sst3])

      assert {1, [^sst1]} = Store.get(__MODULE__.Store, 1)
      assert {2, ssts} = Store.get(__MODULE__.Store, 2)
      assert Enum.sort_by(ssts, & &1.file) == Enum.sort_by([sst1, sst3], & &1.file)
      assert {3, ssts} = Store.get(__MODULE__.Store, 3)
      assert Enum.sort_by(ssts, & &1.file) == Enum.sort_by([sst2, sst3], & &1.file)
      assert {4, [^sst2]} = Store.get(__MODULE__.Store, 4)
    end

    test "returns iterators over all SSTs when min and max is not specified", c do
      file1 = Path.join(c.tmp_dir, "foo")
      sst1 = fake_sst(file1, [{1, 0, "v-1"}, {2, 1, "v-2"}])
      file2 = Path.join(c.tmp_dir, "bar")
      sst2 = fake_sst(file2, [{3, 2, "v-3"}, {4, 3, "v-4"}])
      file3 = Path.join(c.tmp_dir, "baz")
      sst3 = fake_sst(file3, [{2, 4, "w-2"}, {3, 5, "w-3"}])
      Store.put(c.store, [sst1])
      Store.put(c.store, [sst2])
      Store.put(c.store, [sst3])

      assert [_, _, _] = Store.iterators(__MODULE__.Store, nil, nil)
    end

    test "returns iterators over relevant SSTs", c do
      file1 = Path.join(c.tmp_dir, "foo")
      sst1 = fake_sst(file1, [{1, 0, "v-1"}, {2, 1, "v-2"}])
      file2 = Path.join(c.tmp_dir, "bar")
      sst2 = fake_sst(file2, [{3, 2, "v-3"}, {4, 3, "v-4"}])
      file3 = Path.join(c.tmp_dir, "baz")
      sst3 = fake_sst(file3, [{2, 4, "w-2"}, {3, 5, "w-3"}])
      Store.put(c.store, [sst1])
      Store.put(c.store, [sst2])
      Store.put(c.store, [sst3])

      assert [_] = Store.iterators(__MODULE__.Store, 1, 1)
      assert [_, _, _] = Store.iterators(__MODULE__.Store, 2, 3)
      assert [_, _, _] = Store.iterators(__MODULE__.Store, 1, 4)
    end
  end
end
