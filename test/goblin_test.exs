defmodule GoblinTest do
  use ExUnit.Case, async: true
  use TestHelper

  @moduletag :tmp_dir

  describe "start_link/1" do
    test "can start multiple independent databases simultaneously", c do
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

  describe "stop/3" do
    test "stops started database", c do
      {:ok, db} = Goblin.start_link(db_dir: c.tmp_dir)
      assert :ok == Goblin.stop(db)
      refute Process.alive?(db)
    end
  end

  describe "transaction/2" do
    setup_db(key_limit: 10, level_limit: 512)

    test "is atomic", c do
      parent = self()

      assert nil == Goblin.get(c.db, :k)

      pid =
        spawn(fn ->
          assert :ok ==
                   Goblin.transaction(c.db, fn tx ->
                     tx = Goblin.Tx.put(tx, :k, :v)
                     send(parent, :ready)

                     receive do
                       :cont -> :ok
                     end

                     {:commit, tx, :ok}
                   end)

          send(parent, :done)
        end)

      receive do
        :ready -> :ok
      end

      assert nil == Goblin.get(c.db, :k)

      send(pid, :cont)

      assert_receive :done
      assert :v == Goblin.get(c.db, :k)
    end

    test "is consistent", c do
      Goblin.put(c.db, :counter, 10)

      for _n <- 1..20 do
        Task.async(fn ->
          assert :ok ==
                   Goblin.transaction(c.db, fn tx ->
                     counter = Goblin.Tx.get(tx, :counter, 0)

                     if counter > 0 do
                       tx = Goblin.Tx.put(tx, :counter, counter - 1)
                       {:commit, tx, :ok}
                     else
                       :cancel
                     end
                   end)
        end)
      end
      |> Task.await_many()

      counter = Goblin.get(c.db, :counter)
      assert counter >= 0
    end

    test "is isolated", c do
      parent = self()
      Goblin.put(c.db, :k1, 0)
      Goblin.put(c.db, :k2, 0)

      pid1 =
        spawn(fn ->
          assert :ok ==
                   Goblin.transaction(c.db, fn tx ->
                     send(parent, :ready)

                     tx = Goblin.Tx.put(tx, :k1, 100)

                     receive do
                       :cont -> :ok
                     end

                     tx = Goblin.Tx.put(tx, :k2, 100)
                     x1 = Goblin.Tx.get(tx, :k1)
                     x2 = Goblin.Tx.get(tx, :k2)
                     send(parent, {:keys, x1, x2})
                     {:commit, tx, :ok}
                   end)

          send(parent, :done1)
        end)

      receive do
        :ready -> :ok
      end

      spawn(fn ->
        assert :ok ==
                 Goblin.transaction(c.db, fn tx ->
                   tx =
                     tx
                     |> Goblin.Tx.put(:k1, 50)
                     |> Goblin.Tx.put(:k2, 50)

                   {:commit, tx, :ok}
                 end)

        send(parent, :done2)
      end)

      send(pid1, :cont)

      assert_receive {:keys, 100, 100}
      assert_receive :done1
      assert_receive :done2
      assert 50 == Goblin.get(c.db, :k1)
      assert 50 == Goblin.get(c.db, :k2)
    end

    test "is durable", c do
      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 tx =
                   tx
                   |> Goblin.Tx.put(:k, :v)
                   |> Goblin.Tx.put(:l, :w)

                 {:commit, tx, :ok}
               end)

      assert :v == Goblin.get(c.db, :k)
      assert :w == Goblin.get(c.db, :l)

      stop_db(__MODULE__)
      %{db: db} = start_db(c.tmp_dir, name: __MODULE__)

      assert :v == Goblin.get(db, :k)
      assert :w == Goblin.get(db, :l)
    end
  end

  describe "export/2" do
    setup_db(key_limit: 10, level_limit: 512)

    setup c do
      export_dir = Path.join(c.tmp_dir, "exports")
      unpack_dir = Path.join(c.tmp_dir, "unpack")
      File.mkdir!(export_dir)
      File.mkdir!(unpack_dir)
      %{export_dir: export_dir, unpack_dir: unpack_dir}
    end

    test "exports snapshot of database", c do
      Goblin.put(c.db, :k1, :v1)
      Goblin.put(c.db, :k2, :v2)

      assert {:ok, tar_name} = Goblin.export(c.db, c.export_dir)

      :ok = :erl_tar.extract(~c"#{tar_name}", [:compressed, cwd: ~c"#{c.unpack_dir}"])

      Goblin.remove(c.db, :k2)

      assert {:ok, backup_db} = Goblin.start_link(name: Goblin.Backup, db_dir: c.unpack_dir)

      assert :v1 == Goblin.get(backup_db, :k1)
      assert :v2 == Goblin.get(backup_db, :k2)

      assert :v1 == Goblin.get(c.db, :k1)
      assert nil == Goblin.get(c.db, :k2)
    end
  end
end
