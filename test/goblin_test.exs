defmodule GoblinTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog

  describe "start_link/1" do
    @describetag :tmp_dir

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
    @describetag :tmp_dir

    test "stops started database", c do
      {:ok, db} = Goblin.start_link(db_dir: c.tmp_dir)
      assert :ok == Goblin.stop(db)
      refute Process.alive?(db)
    end
  end

  describe "atomicity" do
    setup_db()

    test "writes are only visible after commit", c do
      parent = self()

      assert nil == Goblin.get(c.db, :key1)
      assert nil == Goblin.get(c.db, :key2)
      assert nil == Goblin.get(c.db, :key3)
      assert [] == Goblin.get_multi(c.db, [:key1, :key2, :key3])
      assert [] == Goblin.select(c.db) |> Enum.to_list()

      writer =
        spawn(fn ->
          assert :ok ==
                   Goblin.transaction(c.db, fn tx ->
                     tx =
                       tx
                       |> Goblin.Tx.put(:key1, :val1)
                       |> Goblin.Tx.put_multi([{:key2, :val2}, {:key3, :val3}])
                       |> Goblin.Tx.remove(:key3)

                     send(parent, :ready)

                     receive do
                       :cont -> :ok
                     end

                     {:commit, tx, :ok}
                   end)

          send(parent, :done)
        end)

      assert_receive :ready

      assert nil == Goblin.get(c.db, :key1)
      assert nil == Goblin.get(c.db, :key2)
      assert nil == Goblin.get(c.db, :key3)
      assert [] == Goblin.get_multi(c.db, [:key1, :key2, :key3])
      assert [] == Goblin.select(c.db) |> Enum.to_list()

      send(writer, :cont)

      assert_receive :done

      assert :val1 == Goblin.get(c.db, :key1)
      assert :val2 == Goblin.get(c.db, :key2)
      assert nil == Goblin.get(c.db, :key3)
      assert [{:key1, :val1}, {:key2, :val2}] == Goblin.get_multi(c.db, [:key1, :key2, :key3])
      assert [{:key1, :val1}, {:key2, :val2}] == Goblin.select(c.db) |> Enum.to_list()
    end

    test "aborted commits cannot be read", c do
      parent = self()

      spawn(fn ->
        assert {:error, :aborted} ==
                 Goblin.transaction(c.db, fn tx ->
                   Goblin.Tx.put(tx, :key, :val)
                   :abort
                 end)

        send(parent, :done)
      end)

      assert_receive :done

      refute :val == Goblin.get(c.db, :key)
    end

    test "concurrent reads cannot read from writing transaction", c do
      parent = self()

      writer =
        spawn(fn ->
          Goblin.transaction(c.db, fn tx ->
            tx = Goblin.Tx.put(tx, :key, :val)

            send(parent, :ready)

            receive do
              :cont -> :ok
            end

            tx = Goblin.Tx.put(tx, :key, :another_val)
            {:commit, tx, :ok}
          end)

          send(parent, :done)
        end)

      assert_receive :ready

      reader =
        spawn(fn ->
          Goblin.read(c.db, fn tx ->
            send(parent, :ready)

            assert nil == Goblin.Tx.get(tx, :key)

            receive do
              :cont -> :ok
            end

            assert nil == Goblin.Tx.get(tx, :key)
          end)

          send(parent, :done)
        end)

      assert_receive :ready

      send(writer, :cont)
      assert_receive :done

      send(reader, :cont)
      assert_receive :done
    end
  end

  describe "consistency" do
    setup_db()

    test "can maintain a business rule", c do
      Goblin.put(c.db, :counter, 10)

      results =
        for _n <- 1..20 do
          Task.async(fn ->
            Goblin.transaction(c.db, fn tx ->
              counter = Goblin.Tx.get(tx, :counter, 0)

              if counter > 0 do
                tx = Goblin.Tx.put(tx, :counter, counter - 1)
                {:commit, tx, :ok}
              else
                :abort
              end
            end)
          end)
        end
        |> Task.await_many()

      assert 10 == length(Enum.filter(results, &match?(:ok, &1)))
      assert 10 == length(Enum.filter(results, &match?({:error, :aborted}, &1)))
      assert 0 == Goblin.get(c.db, :counter)
    end
  end

  describe "isolated" do
    setup_db()

    test "transaction wait upon each other (truly serializable)", c do
      parent = self()

      spawn(fn ->
        assert :ok ==
                 Goblin.transaction(c.db, fn tx ->
                   send(parent, :tx1_started)

                   Process.sleep(100)

                   tx =
                     tx
                     |> Goblin.Tx.put(:key1, 100)
                     |> Goblin.Tx.put(:key2, 100)

                   send(parent, {:tx1_done, System.monotonic_time()})
                   {:commit, tx, :ok}
                 end)
      end)

      assert_receive :tx1_started

      spawn(fn ->
        send(parent, :tx2_waiting)

        assert :ok ==
                 Goblin.transaction(c.db, fn tx ->
                   send(parent, {:tx2_started, System.monotonic_time()})

                   tx =
                     tx
                     |> Goblin.Tx.put(:key1, 50)
                     |> Goblin.Tx.put(:key2, 50)

                   {:commit, tx, :ok}
                 end)

        send(parent, :tx2_done)
      end)

      assert_receive :tx2_waiting

      tx2_started =
        receive do
          {:tx2_started, tx2_started} -> tx2_started
        end

      tx1_done =
        receive do
          {:tx1_done, tx1_done} -> tx1_done
        end

      assert_receive :tx2_done

      assert tx1_done < tx2_started

      assert 50 == Goblin.get(c.db, :key1)
      assert 50 == Goblin.get(c.db, :key2)
    end

    test "final state matches serial execution", c do
      for n <- 1..20 do
        Task.async(fn ->
          Process.sleep(Enum.random(1..100))

          Goblin.transaction(c.db, fn tx ->
            tx =
              tx
              |> Goblin.Tx.put(:key1, n)
              |> Goblin.Tx.put(:key2, n)

            {:commit, tx, :ok}
          end)
        end)
      end
      |> Task.await_many()

      assert Goblin.get_multi(c.db, [:key1, :key2]) in for(
               n <- 1..20,
               do: [{:key1, n}, {:key2, n}]
             )
    end

    test "readers read from a snapshot", c do
      parent = self()

      Goblin.put(c.db, :key, :val)

      reader =
        spawn(fn ->
          Goblin.read(c.db, fn tx ->
            send(parent, :ready)

            receive do
              :cont -> :ok
            end

            assert :val == Goblin.Tx.get(tx, :key)
          end)

          send(parent, :done)
        end)

      assert_receive :ready

      Goblin.put(c.db, :key, :another_val)

      send(reader, :cont)

      assert_receive :done
    end
  end

  describe "durable" do
    setup_db()

    test "can get same values on restart", c do
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
    setup_db()

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

      {backup_db, _log} =
        with_log(fn ->
          assert {:ok, backup_db} = Goblin.start_link(name: Goblin.Backup, db_dir: c.unpack_dir)
          backup_db
        end)

      assert :v1 == Goblin.get(backup_db, :k1)
      assert :v2 == Goblin.get(backup_db, :k2)

      assert :v1 == Goblin.get(c.db, :k1)
      assert nil == Goblin.get(c.db, :k2)
    end
  end

  describe "subscribe/1, unsubscribe/1" do
    setup_db()

    setup c do
      other_db_dir = Path.join(c.tmp_dir, "other")
      File.mkdir!(other_db_dir)
      %{other_db_dir: other_db_dir}
    end

    test "subscribes only to specific database instance", c do
      assert {:ok, db1} = Goblin.start_link(name: Goblin1, db_dir: c.other_db_dir)

      assert :ok == Goblin.subscribe(c.db)

      Goblin.put(c.db, :k, :v)

      assert_receive {:put, :k, :v}

      Goblin.put(db1, :l, :w)

      refute_receive {:put, :l, :w}
    end

    test "unsubscribes to topic", c do
      assert :ok == Goblin.subscribe(c.db)

      Goblin.put(c.db, :k, :v)

      assert_receive {:put, :k, :v}

      assert :ok == Goblin.unsubscribe(c.db)

      Goblin.put(c.db, :k, :w)

      refute_receive {:put, :k, :w}
    end
  end
end
