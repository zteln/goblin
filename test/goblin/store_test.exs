defmodule Goblin.StoreTest do
  use ExUnit.Case, async: true
  alias Goblin.Store

  @moduletag :tmp_dir

  describe "start_link/1" do
    setup c, do: start_context(c.tmp_dir)

    test "starts with empty state", c do
      assert {:ok, store} =
               Store.start_link(
                 name: __MODULE__,
                 db_dir: c.tmp_dir,
                 manifest: c.manifest,
                 compactor: c.compactor
               )

      assert %{max_file_count: 0, local_name: __MODULE__} = :sys.get_state(store)
    end

    test "recovers state from manifest", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst(file, [{1, 0, "v-1"}, {2, 1, "v-2"}])

      Goblin.Manifest.log_flush(c.manifest, [sst.file], "bar")

      assert {:ok, store} =
               Store.start_link(
                 name: __MODULE__,
                 db_dir: c.tmp_dir,
                 manifest: c.manifest,
                 compactor: c.compactor
               )

      assert %{max_file_count: 1} = :sys.get_state(store)
    end
  end

  describe "put/2, remove/2" do
    setup c, do: start_manifest(c.tmp_dir)

    test "updates store", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst(file, [{1, 0, "v-1"}])

      assert :ok == Store.put(c.store, [sst])

      assert {1, [^sst]} = Store.get(__MODULE__, 1)

      assert :ok == Store.remove(c.store, [file])

      assert {1, []} == Store.get(__MODULE__, 1)
    end

    test "can insert same file multiple times", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst(file, [{1, 0, "v-1"}])

      assert :ok == Store.put(c.store, [sst])
      assert :ok == Store.put(c.store, [sst])
      assert :ok == Store.put(c.store, [sst])
      assert {1, [^sst]} = Store.get(__MODULE__, 1)
    end

    test "can remove multiple times", c do
      assert :ok == Store.remove(c.store, ["foo"])
      assert :ok == Store.remove(c.store, ["foo"])
      assert :ok == Store.remove(c.store, ["foo"])
      assert {1, []} == Store.get(__MODULE__, 1)
    end
  end

  describe "new_file/1" do
    setup c, do: start_manifest(c.tmp_dir)

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
    setup c, do: start_manifest(c.tmp_dir)

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

      assert {1, [^sst1]} = Store.get(__MODULE__, 1)
      assert {2, ssts} = Store.get(__MODULE__, 2)
      assert Enum.sort_by(ssts, & &1.file) == Enum.sort_by([sst1, sst3], & &1.file)
      assert {3, ssts} = Store.get(__MODULE__, 3)
      assert Enum.sort_by(ssts, & &1.file) == Enum.sort_by([sst2, sst3], & &1.file)
      assert {4, [^sst2]} = Store.get(__MODULE__, 4)
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

      assert [_, _, _] = Store.iterators(__MODULE__, nil, nil)
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

      assert [_] = Store.iterators(__MODULE__, 1, 1)
      assert [_, _, _] = Store.iterators(__MODULE__, 2, 3)
      assert [_, _, _] = Store.iterators(__MODULE__, 1, 4)
    end
  end

  defp start_context(dir) do
    compactor =
      start_link_supervised!(
        {Goblin.Compactor,
         name: __MODULE__.Compactor,
         store: __MODULE__,
         manifest: __MODULE__.Manifest,
         task_sup: __MODULE__.TaskSupervisor,
         key_limit: 10,
         level_limit: 1024},
        id: __MODULE__.Compactor
      )

    manifest =
      start_link_supervised!({Goblin.Manifest, name: __MODULE__.Manifest, db_dir: dir},
        id: __MODULE__.Manifest
      )

    start_link_supervised!({Task.Supervisor, name: __MODULE__.TaskSupervisor},
      id: __MODULE__.TaskSupervisor
    )

    %{compactor: compactor, manifest: manifest}
  end

  defp start_manifest(dir) do
    start_context(dir)

    store =
      start_link_supervised!(
        {Store,
         name: __MODULE__,
         db_dir: dir,
         manifest: __MODULE__.Manifest,
         compactor: __MODULE__.Compactor},
        id: __MODULE__
      )

    %{store: store}
  end

  defp fake_sst(file, data) do
    {:ok, [sst]} =
      Goblin.SSTs.new([[data]], 0, file_getter: fn -> file end)

    sst
  end
end
