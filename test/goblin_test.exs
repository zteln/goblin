defmodule GoblinTest do
  use ExUnit.Case, async: true
  use TestHelper

  @moduletag :tmp_dir
  @db_opts [
    id: :goblin_test,
    name: __MODULE__,
    key_limit: 10,
    sync_interval: 50,
    wal_name: :goblin_test_wal,
    manifest_name: :goblin_test_manifest
  ]

  describe "start_link/1" do
    test "starts database", c do
      assert {:ok, _db} = Goblin.start_link(db_dir: c.tmp_dir)
    end

    test "fails if no db_dir provided" do
      assert_raise RuntimeError, "no db_dir provided.", fn ->
        Goblin.start_link([])
      end
    end

    test "can start multiple databases simultaneously", c do
      dir1 = Path.join(c.tmp_dir, "dir1")
      dir2 = Path.join(c.tmp_dir, "dir2")
      dir3 = Path.join(c.tmp_dir, "dir3")

      assert {:ok, db1} = Goblin.start_link(name: DB1, db_dir: dir1)
      assert {:ok, db2} = Goblin.start_link(name: DB2, db_dir: dir2)
      assert {:ok, db3} = Goblin.start_link(name: DB3, db_dir: dir3)

      assert Process.alive?(db1)
      assert Process.alive?(db2)
      assert Process.alive?(db3)

      assert :ok == Goblin.put(db1, :k, :v)
      assert :v == Goblin.get(db1, :k)
      assert nil == Goblin.get(db2, :k)
      assert nil == Goblin.get(db3, :k)
    end
  end

  describe "transaction/2" do
    setup_db(@db_opts)

    test "is atomic", c do
      assert [] == Goblin.get_multi(c.db, [:k, :l])

      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 tx = Goblin.Tx.put(tx, :k, :v)
                 tx = Goblin.Tx.put(tx, :l, :w)
                 {:commit, tx, :ok}
               end)

      assert [{:k, :v}, {:l, :w}] == Goblin.get_multi(c.db, [:k, :l])

      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 tx = Goblin.Tx.put(tx, :k, :u)
                 Goblin.Tx.put(tx, :l, :x)
                 :cancel
               end)

      assert [{:k, :v}, {:l, :w}] == Goblin.get_multi(c.db, [:k, :l])
    end

    test "is consistent", c do
      assert nil == Goblin.get(c.db, :k)

      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 tx = Goblin.Tx.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end)

      assert :v == Goblin.get(c.db, :k)

      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 Goblin.Tx.put(tx, :k, :w)
                 :cancel
               end)

      refute :w == Goblin.get(c.db, :k)
      assert :v == Goblin.get(c.db, :k)
    end

    test "are isolated", c do
      assert nil == Goblin.get(c.db, :k)

      parent = self()

      pid1 =
        spawn(fn ->
          assert :ok ==
                   Goblin.transaction(c.db, fn tx ->
                     tx = Goblin.Tx.put(tx, :k, :v)

                     send(parent, :cont)

                     receive do
                       :cont -> :ok
                     end

                     {:commit, tx, :ok}
                   end)
        end)

      receive do
        :cont -> :ok
      end

      assert nil == Goblin.get(c.db, :k)

      send(pid1, :cont)

      assert_eventually do
        assert :v == Goblin.get(c.db, :k)
      end
    end

    test "are durable", c do
      assert nil == Goblin.get(c.db, :k)

      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 tx = Goblin.Tx.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end)

      assert :v == Goblin.get(c.db, :k)

      Goblin.WAL.sync(c.registry)
      stop_supervised!(c.db_id)
      %{db: db} = start_db(c.tmp_dir, @db_opts)

      assert :v == Goblin.get(db, :k)
    end
  end

  describe "put/3, put_multi/2, remove/2, remove_multi/2, get/3, get_multi/3" do
    setup_db(@db_opts)

    test "inserts key_values and is available for reads immediately", c do
      assert :ok == Goblin.put(c.db, :k, :v)
      assert :v == Goblin.get(c.db, :k)
      assert :ok == Goblin.put_multi(c.db, k1: :v1, k2: :v2, k3: :v3)
      assert [k1: :v1, k2: :v2, k3: :v3] == Goblin.get_multi(c.db, [:k1, :k2, :k3])
    end

    test "multiple puts flushes writes to disk when exceeding key limit", c do
      no_of_files = File.ls!(c.tmp_dir) |> length()
      assert :ok == Goblin.put_multi(c.db, for(n <- 1..10, do: {n, "v-#{n}"}))

      assert_eventually do
        assert no_of_files + 1 == File.ls!(c.tmp_dir) |> length()
      end
    end

    test "multiple removes flushes to disk when exceeding key limit", c do
      no_of_files = File.ls!(c.tmp_dir) |> length()
      assert :ok == Goblin.remove_multi(c.db, Enum.to_list(1..10))

      assert_eventually do
        assert no_of_files + 1 == File.ls!(c.tmp_dir) |> length()
      end
    end

    test "does not resurrect previous writes", c do
      Goblin.put_multi(c.db, for(n <- 1..10, do: {n, "v-#{n}"}))
      assert "v-1" == Goblin.get(c.db, 1)
      assert :ok == Goblin.remove(c.db, 1)
      assert nil == Goblin.get(c.db, 1)
      assert :ok == Goblin.remove_multi(c.db, Enum.to_list(1..10))
      assert [] == Goblin.get_multi(c.db, Enum.to_list(1..10))
    end
  end

  describe "select/2" do
    setup_db(@db_opts)

    test "returns a stream of key-value pairs", c do
      Goblin.put_multi(c.db, for(n <- 1..10, do: {n, "v-#{n}"}))

      assert for(n <- 1..10, do: {n, "v-#{n}"}) == Goblin.select(c.db) |> Enum.to_list()

      assert for(n <- 2..5, do: {n, "v-#{n}"}) ==
               Goblin.select(c.db, min: 2, max: 5) |> Enum.to_list()

      assert for(n <- 2..10, do: {n, "v-#{n}"}) == Goblin.select(c.db, min: 2) |> Enum.to_list()
      assert for(n <- 1..5, do: {n, "v-#{n}"}) == Goblin.select(c.db, max: 5) |> Enum.to_list()
    end
  end

  describe "is_compacting/1, is_flushing/1" do
    setup_db(@db_opts)

    defmodule FakeTask do
      def async(_, _), do: %{ref: make_ref()}
    end

    @tag db_opts: [task_mod: FakeTask]
    test "is_flushing/1 returns true when flushing, false otherwise", c do
      refute Goblin.is_flushing(c.db)
      Goblin.put_multi(c.db, for(n <- 1..10, do: {n, "v-#{n}"}))
      assert Goblin.is_flushing(c.db)
    end
  end

  describe "subscribe/1, unsubscribe/1" do
    setup_db(@db_opts)

    test "subscribed process receives write updates", c do
      assert :ok == Goblin.subscribe(c.db)

      Goblin.put(c.db, :k, :v)

      assert_receive {:put, :k, :v}

      Goblin.remove(c.db, :k)

      assert_receive {:remove, :k}
    end

    test "unsubscribed process receives no updates", c do
      assert :ok == Goblin.subscribe(c.db)

      Goblin.put(c.db, :k, :v)

      assert_receive {:put, :k, :v}

      assert :ok == Goblin.unsubscribe(c.db)

      Goblin.remove(c.db, :k)

      refute_receive {:remove, :k}
    end

    test "pubsub is separate from different databases", c do
      parent = self()
      {:ok, db} = Goblin.start_link(name: Foo, db_dir: Path.join(c.tmp_dir, "foo"))

      spawn(fn ->
        assert :ok == Goblin.subscribe(c.db)

        assert_receive {:put, :k1, :v1}
        refute_receive {:remove, :k2}

        send(parent, :done1)
      end)

      spawn(fn ->
        assert :ok == Goblin.subscribe(db)

        refute_receive {:put, :k1, :v1}
        assert_receive {:remove, :k2}

        send(parent, :done2)
      end)

      Goblin.put(c.db, :k1, :v1)
      Goblin.remove(db, :k2)

      assert_receive :done1, 200
      assert_receive :done2, 200
    end
  end
end
