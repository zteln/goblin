defmodule GoblinTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :tmp_dir

  @default_opts [mem_limit: 2 * 1024, bit_array_size: 1000]

  describe "start_link/1, start/1, stop/3" do
    test "can start multiple independent databases simultaneously", ctx do
      dir1 = Path.join(ctx.tmp_dir, "dir1")
      dir2 = Path.join(ctx.tmp_dir, "dir2")
      dir3 = Path.join(ctx.tmp_dir, "dir3")

      assert {:ok, db1} = Goblin.start(name: DB1, data_dir: dir1)
      assert {:ok, db2} = Goblin.start(name: DB2, data_dir: dir2)
      assert {:ok, db3} = Goblin.start(name: DB3, data_dir: dir3)

      assert Process.alive?(db1)
      assert Process.alive?(db2)
      assert Process.alive?(db3)

      assert :ok == Goblin.put(db1, :k, :v)
      assert :v == Goblin.get(db1, :k)
      assert nil == Goblin.get(db2, :k)
      assert nil == Goblin.get(db3, :k)

      Goblin.stop(db1)
      Goblin.stop(db2)
      Goblin.stop(db3)
    end

    test "creates link via start_link/1", ctx do
      assert {:ok, db} = Goblin.start_link(data_dir: ctx.tmp_dir)
      assert db in (Process.info(self()) |> Keyword.get(:links))
    end

    test "stops started database", ctx do
      {:ok, db} = Goblin.start(data_dir: ctx.tmp_dir)
      assert :ok == Goblin.stop(db)
      refute Process.alive?(db)
    end
  end

  describe "atomicity" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "writes are only visible after commit", ctx do
      parent = self()

      assert nil == Goblin.get(ctx.db, :key1)
      assert nil == Goblin.get(ctx.db, :key2)
      assert nil == Goblin.get(ctx.db, :key3)
      assert [] == Goblin.get_multi(ctx.db, [:key1, :key2, :key3])
      assert [] == Goblin.scan(ctx.db) |> Enum.to_list()

      writer =
        spawn(fn ->
          assert :ok ==
                   Goblin.transaction(ctx.db, fn tx ->
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

      assert_receive :ready, 5000

      assert nil == Goblin.get(ctx.db, :key1)
      assert nil == Goblin.get(ctx.db, :key2)
      assert nil == Goblin.get(ctx.db, :key3)
      assert [] == Goblin.get_multi(ctx.db, [:key1, :key2, :key3])
      assert [] == Goblin.scan(ctx.db) |> Enum.to_list()

      send(writer, :cont)

      assert_receive :done, 5000

      assert :val1 == Goblin.get(ctx.db, :key1)
      assert :val2 == Goblin.get(ctx.db, :key2)
      assert nil == Goblin.get(ctx.db, :key3)

      assert %{key1: :val1, key2: :val2} ==
               Map.new(Goblin.get_multi(ctx.db, [:key1, :key2, :key3]))

      assert [{:key1, :val1}, {:key2, :val2}] == Goblin.scan(ctx.db) |> Enum.to_list()
    end

    test "aborted commits cannot be read", ctx do
      assert :error ==
               Goblin.transaction(ctx.db, fn tx ->
                 tx
                 |> Goblin.Tx.put(:key, :val)
                 |> Goblin.Tx.abort()
               end)

      assert nil == Goblin.get(ctx.db, :key)
    end

    test "exception in callback aborts the transaction", ctx do
      assert_raise RuntimeError, fn ->
        Goblin.transaction(ctx.db, fn tx ->
          Goblin.Tx.put(tx, :key, :val)
          raise "boom"
        end)
      end

      assert nil == Goblin.get(ctx.db, :key)

      # Verify the database is still functional
      assert :ok == Goblin.put(ctx.db, :after_crash, :works)
      assert :works == Goblin.get(ctx.db, :after_crash)
    end

    test "concurrent reads cannot read from writing transaction", ctx do
      parent = self()

      writer =
        spawn(fn ->
          Goblin.transaction(ctx.db, fn tx ->
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

      assert_receive :ready, 5000

      reader =
        spawn(fn ->
          Goblin.read(ctx.db, fn tx ->
            send(parent, :ready)

            assert nil == Goblin.Tx.get(tx, :key)

            receive do
              :cont -> :ok
            end

            assert nil == Goblin.Tx.get(tx, :key)
          end)

          send(parent, :done)
        end)

      assert_receive :ready, 5000

      send(writer, :cont)
      assert_receive :done, 5000

      send(reader, :cont)
      assert_receive :done, 5000
    end

    @tag :property_tests
    property "a transaction applies all of its writes or none", ctx do
      check all(
              ops <- list_of(operation_gen(), min_length: 1, max_length: 20),
              outcome <- member_of([:commit, :abort, :raise])
            ) do
        tag = make_ref()

        try do
          Goblin.transaction(ctx.db, fn tx ->
            tx =
              Enum.reduce(ops, tx, fn
                {:put, k, v}, acc -> Goblin.Tx.put(acc, k, v, tag: tag)
                {:remove, k}, acc -> Goblin.Tx.remove(acc, k, tag: tag)
              end)

            case outcome do
              :commit -> Goblin.Tx.commit(tx)
              :abort -> Goblin.Tx.abort(tx)
              :raise -> raise "error"
            end
          end)
        rescue
          RuntimeError -> :raised
        end

        ops = if outcome == :commit, do: ops, else: []

        expected =
          Enum.reduce(ops, [], fn
            {:put, k, v}, acc -> [{k, v} | Enum.reject(acc, &(elem(&1, 0) == k))]
            {:remove, k}, acc -> Enum.reject(acc, &(elem(&1, 0) == k))
          end)
          |> Map.new()

        actual = Goblin.scan(ctx.db, tag: tag) |> Enum.to_list() |> Map.new()
        assert actual == expected
      end
    end
  end

  describe "consistency" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "can maintain a business rule", ctx do
      Goblin.put(ctx.db, :counter, 10)

      results =
        for _n <- 1..20 do
          Task.async(fn ->
            Goblin.transaction(ctx.db, fn tx ->
              counter = Goblin.Tx.get(tx, :counter, default: 0)

              if counter > 0 do
                tx = Goblin.Tx.put(tx, :counter, counter - 1)
                {:commit, tx, :ok}
              else
                {:abort, :error}
              end
            end)
          end)
        end
        |> Task.await_many()

      assert 10 == length(Enum.filter(results, &match?(:ok, &1)))
      assert 10 == length(Enum.filter(results, &match?(:error, &1)))
      assert 0 == Goblin.get(ctx.db, :counter)
    end

    @tag :property_tests
    property "concurrent transfers conserve the total balance", ctx do
      check all(
              n_accounts <- integer(2..6),
              transfers <- list_of(transfer_gen(), min_length: 1, max_length: 30),
              max_runs: 25
            ) do
        tag = make_ref()
        Goblin.put_multi(ctx.db, for(i <- 1..n_accounts, do: {i, 100}), tag: tag)
        total = n_accounts * 100

        transfers
        |> Enum.map(fn {from, to, amount} ->
          from = rem(from, n_accounts) + 1
          to = rem(to, n_accounts) + 1

          Task.async(fn ->
            Goblin.transaction(ctx.db, fn tx ->
              from_bal = Goblin.Tx.get(tx, from, tag: tag)
              to_bal = Goblin.Tx.get(tx, to, tag: tag)

              if from != to and from_bal >= amount do
                tx =
                  tx
                  |> Goblin.Tx.put(from, from_bal - amount, tag: tag)
                  |> Goblin.Tx.put(to, to_bal + amount, tag: tag)

                {:commit, tx, :ok}
              else
                {:abort, :error}
              end
            end)
          end)
        end)
        |> Task.await_many()

        balances = for i <- 1..n_accounts, do: Goblin.get(ctx.db, i, tag: tag)

        # quantity conserved
        assert Enum.sum(balances) == total
        # never overdrawn
        assert Enum.all?(balances, &(&1 >= 0))
      end
    end

    defp transfer_gen do
      gen all(from <- positive_integer(), to <- positive_integer(), amount <- integer(1..50)) do
        {from, to, amount}
      end
    end
  end

  describe "isolated" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "transactions wait upon each other (truly serializable)", ctx do
      parent = self()

      spawn(fn ->
        assert :ok ==
                 Goblin.transaction(ctx.db, fn tx ->
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
                 Goblin.transaction(ctx.db, fn tx ->
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

      assert 50 == Goblin.get(ctx.db, :key1)
      assert 50 == Goblin.get(ctx.db, :key2)
    end

    test "final state matches serial execution", ctx do
      for n <- 1..20 do
        Task.async(fn ->
          Process.sleep(Enum.random(1..100))

          Goblin.transaction(ctx.db, fn tx ->
            tx
            |> Goblin.Tx.put(:key1, n)
            |> Goblin.Tx.put(:key2, n)
            |> Goblin.Tx.commit()
          end)
        end)
      end
      |> Task.await_many()

      assert Map.new(Goblin.get_multi(ctx.db, [:key1, :key2])) in for(
               n <- 1..20,
               do: %{key1: n, key2: n}
             )
    end

    test "readers read from a snapshot", ctx do
      parent = self()

      Goblin.put(ctx.db, :key, :val)

      reader =
        spawn(fn ->
          Goblin.read(ctx.db, fn tx ->
            send(parent, :ready)

            receive do
              :cont -> :ok
            end

            assert :val == Goblin.Tx.get(tx, :key)
          end)

          send(parent, :done)
        end)

      assert_receive :ready

      Goblin.put(ctx.db, :key, :another_val)

      send(reader, :cont)

      assert_receive :done
    end

    @tag :property_tests
    property "concurrent increments never lose an update", ctx do
      check all(n <- integer(1..25), max_runs: 25) do
        tag = make_ref()
        Goblin.put(ctx.db, :counter, 0, tag: tag)

        for _ <- 1..n do
          Task.async(fn ->
            Goblin.transaction(ctx.db, fn tx ->
              c = Goblin.Tx.get(tx, :counter, tag: tag)
              {:commit, Goblin.Tx.put(tx, :counter, c + 1, tag: tag), :ok}
            end)
          end)
        end
        |> Task.await_many()

        assert n == Goblin.get(ctx.db, :counter, tag: tag)
      end
    end
  end

  describe "durable" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "fresh start does not expose uncommitted writes to concurrent readers", ctx do
      parent = self()

      writer =
        spawn(fn ->
          Goblin.transaction(ctx.db, fn tx ->
            tx = Goblin.Tx.put(tx, :key, :uncommitted)
            send(parent, :in_tx)

            receive do
              :cont -> {:commit, tx, :ok}
            end
          end)

          send(parent, :committed)
        end)

      assert_receive :in_tx, 5000

      assert nil == Goblin.get(ctx.db, :key)

      send(writer, :cont)
      assert_receive :committed, 5000

      assert :uncommitted == Goblin.get(ctx.db, :key)
    end

    test "can get same values on restart", ctx do
      assert :ok ==
               Goblin.transaction(ctx.db, fn tx ->
                 tx
                 |> Goblin.Tx.put(:k, :v)
                 |> Goblin.Tx.put(:l, :w)
                 |> Goblin.Tx.commit()
               end)

      assert :v == Goblin.get(ctx.db, :k)
      assert :w == Goblin.get(ctx.db, :l)

      stop_supervised(__MODULE__)
      {:ok, db} = start_supervised({Goblin, [data_dir: ctx.tmp_dir] ++ @default_opts})

      assert :v == Goblin.get(db, :k)
      assert :w == Goblin.get(db, :l)
    end

    @tag :property_tests
    property "committed writes survive a crash and restart", ctx do
      check all(
              batches <-
                list_of(list_of(operation_gen(), min_length: 1, max_length: 10),
                  min_length: 1,
                  max_length: 5
                ),
              max_runs: 20
            ) do
        id = System.unique_integer([:positive])
        db_name = :"#{ctx.test}_#{id}"
        data_dir = Path.join(ctx.tmp_dir, "durable_#{id}")

        start = fn ->
          {:ok, db} = Goblin.start([name: db_name, data_dir: data_dir] ++ @default_opts)
          db
        end

        db = start.()

        model =
          Enum.reduce(batches, %{}, fn ops, model ->
            Goblin.transaction(db, fn tx ->
              Enum.reduce(ops, tx, fn
                {:put, k, v}, acc -> Goblin.Tx.put(acc, k, v)
                {:remove, k}, acc -> Goblin.Tx.remove(acc, k)
              end)
              |> Goblin.Tx.commit()
            end)

            Enum.reduce(ops, model, fn
              {:put, k, v}, acc -> [{k, v} | Enum.reject(acc, &(elem(&1, 0) == k))]
              {:remove, k}, acc -> Enum.reject(acc, &(elem(&1, 0) == k))
            end)
            |> Map.new()
          end)

        kill_db(db)
        db = start.()

        actual = Goblin.scan(db) |> Enum.to_list() |> Map.new()
        assert actual == model

        Goblin.stop(db)
      end
    end
  end

  describe "writes" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "update sets default if key is not set", ctx do
      assert nil == Goblin.get(ctx.db, :a)
      assert :ok == Goblin.update(ctx.db, :a, &(&1 + 1), default: 1)
      assert 1 == Goblin.get(ctx.db, :a)
    end

    test "update updates with updating function", ctx do
      assert :ok == Goblin.update(ctx.db, :a, &(&1 + 1), default: 1)
      assert :ok == Goblin.update(ctx.db, :a, &(&1 + 1), default: 1)
      assert :ok == Goblin.update(ctx.db, :a, &(&1 + 1), default: 1)
      assert 3 == Goblin.get(ctx.db, :a)
    end

    test "update_multi updates all provided keys", ctx do
      Goblin.put_multi(ctx.db, %{a: 1, b: 1, c: 1, d: 1})
      assert {:ok, 3} == Goblin.update_multi(ctx.db, [:a, :b, :c], &(&1 + 1))

      assert %{a: 2, b: 2, c: 2, d: 1} ==
               Goblin.get_multi(ctx.db, [:a, :b, :c, :d]) |> Enum.into(%{})
    end

    test "update_multi does not update non-existing keys", ctx do
      Goblin.put_multi(ctx.db, %{a: 1, b: 1})
      assert nil == Goblin.get(ctx.db, :c)
      assert {:ok, 2} == Goblin.update_multi(ctx.db, [:a, :b, :c], &(&1 + 1))

      assert %{a: 2, b: 2} ==
               Goblin.get_multi(ctx.db, [:a, :b, :c]) |> Enum.into(%{})
    end

    test "update_multi inserts default value for non-existing keys", ctx do
      assert {:ok, 2} == Goblin.update_multi(ctx.db, [:a, :b], &(&1 + 1), default: 1)

      assert %{a: 1, b: 1} ==
               Goblin.get_multi(ctx.db, [:a, :b]) |> Enum.into(%{})
    end

    test "update_multi pass arity 1 or 2 update functions", ctx do
      Goblin.put_multi(ctx.db, %{a: 1, b: 1})

      assert {:ok, 2} ==
               Goblin.update_multi(ctx.db, [:a, :b], fn key, val ->
                 assert key in [:a, :b]
                 assert val == 1
                 2
               end)
    end

    test "get_and_update passes nil if key is not set to updater", ctx do
      assert :ok ==
               Goblin.get_and_update(ctx.db, :a, fn val ->
                 assert is_nil(val)
                 {:ok, val}
               end)
    end

    test "get_and_update_multi gets all keys and values, exluding non-existing keys", ctx do
      Goblin.put_multi(ctx.db, %{a: 1, b: 1})

      assert :ok ==
               Goblin.get_and_update_multi(ctx.db, [:a, :b, :c], fn entries ->
                 assert MapSet.new([:a, :b]) == Map.keys(entries) |> MapSet.new()
                 entries = Enum.into(entries, %{}, fn {k, v} -> {k, v + 1} end)
                 {:ok, entries}
               end)

      assert %{a: 2, b: 2} == Goblin.get_multi(ctx.db, [:a, :b]) |> Enum.into(%{})
    end

    test "cas returns boolean indicating if swapped or not", ctx do
      Goblin.put(ctx.db, :counter, 1)
      assert Goblin.cas(ctx.db, :counter, 1, 2)
      refute Goblin.cas(ctx.db, :counter, 1, 2)
      assert Goblin.cas(ctx.db, :counter, 2, 3)
    end
  end

  describe "queries" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "scan respects min/max bounds", ctx do
      Goblin.put_multi(ctx.db, [{1, :a}, {2, :b}, {3, :c}, {4, :d}, {5, :e}])

      assert [{2, :b}, {3, :c}, {4, :d}] == Goblin.scan(ctx.db, min: 2, max: 4) |> Enum.to_list()
      assert [{3, :c}, {4, :d}, {5, :e}] == Goblin.scan(ctx.db, min: 3) |> Enum.to_list()
      assert [{1, :a}, {2, :b}] == Goblin.scan(ctx.db, max: 2) |> Enum.to_list()

      assert [{1, :a}, {2, :b}, {3, :c}, {4, :d}, {5, :e}] ==
               Goblin.scan(ctx.db) |> Enum.to_list()
    end

    test "scan gets snapshots at enumeration", ctx do
      stream = Goblin.scan(ctx.db)
      assert [] == Enum.to_list(stream)
      Goblin.put(ctx.db, 1, :a)
      assert [{1, :a}] == Enum.to_list(stream)
    end

    test "remove then put overwrites tombstone", ctx do
      Goblin.put(ctx.db, :key, :first)
      assert :first == Goblin.get(ctx.db, :key)

      Goblin.remove(ctx.db, :key)
      assert nil == Goblin.get(ctx.db, :key)
      assert [] == Goblin.scan(ctx.db) |> Enum.to_list()

      Goblin.put(ctx.db, :key, :second)
      assert :second == Goblin.get(ctx.db, :key)
      assert [{:key, :second}] == Goblin.scan(ctx.db) |> Enum.to_list()
    end

    test "remove_multi removes multiple keys", ctx do
      Goblin.put_multi(ctx.db, [{:a, 1}, {:b, 2}, {:c, 3}, {:d, 4}])

      Goblin.remove_multi(ctx.db, [:b, :c])

      assert 1 == Goblin.get(ctx.db, :a)
      assert nil == Goblin.get(ctx.db, :b)
      assert nil == Goblin.get(ctx.db, :c)
      assert 4 == Goblin.get(ctx.db, :d)

      assert [{:a, 1}, {:d, 4}] == Goblin.scan(ctx.db) |> Enum.to_list()
    end

    test "get returns default when key is missing", ctx do
      assert nil == Goblin.get(ctx.db, :missing)
      assert :fallback == Goblin.get(ctx.db, :missing, default: :fallback)

      Goblin.put(ctx.db, :exists, :val)
      assert :val == Goblin.get(ctx.db, :exists, default: :fallback)
    end

    test "scan respects tag with min/max bounds", ctx do
      for n <- 1..5 do
        Goblin.put(ctx.db, n, "users_#{n}", tag: :users)
        Goblin.put(ctx.db, n, "orders_#{n}", tag: :orders)
      end

      Goblin.put_multi(ctx.db, [{1, "untagged_1"}, {2, "untagged_2"}])

      # tag + min + max
      result = Goblin.scan(ctx.db, tag: :users, min: 2, max: 4) |> Enum.to_list()
      assert [{2, "users_2"}, {3, "users_3"}, {4, "users_4"}] == result

      # tag + min only
      result = Goblin.scan(ctx.db, tag: :users, min: 4) |> Enum.to_list()
      assert [{4, "users_4"}, {5, "users_5"}] == result

      # tag + max only
      result = Goblin.scan(ctx.db, tag: :users, max: 2) |> Enum.to_list()
      assert [{1, "users_1"}, {2, "users_2"}] == result

      # tag without bounds returns all entries for that tag
      result = Goblin.scan(ctx.db, tag: :orders) |> Enum.to_list()
      assert length(result) == 5
    end

    test "read transaction supports tags", ctx do
      Goblin.put(ctx.db, :key, :tagged_val, tag: :ns)
      Goblin.put(ctx.db, :key, :plain_val)

      Goblin.read(ctx.db, fn tx ->
        assert :tagged_val == Goblin.Tx.get(tx, :key, tag: :ns)
        assert :plain_val == Goblin.Tx.get(tx, :key)
        assert nil == Goblin.Tx.get(tx, :key, tag: :other)

        assert [{:key, :tagged_val}] == Goblin.Tx.get_multi(tx, [:key], tag: :ns)
        assert [{:key, :plain_val}] == Goblin.Tx.get_multi(tx, [:key])
      end)
    end

    test "reads raise during failure to operate on underlying storage", ctx do
      trigger_flush(ctx.db)
      assert :ok == wait_until_idle(ctx.db)

      File.ls!(ctx.tmp_dir)
      |> Enum.filter(&String.ends_with?(&1, ".goblin"))
      |> Enum.map(&Path.join(ctx.tmp_dir, &1))
      |> Enum.each(&File.rm!/1)

      assert_raise Goblin.IOError, fn ->
        Goblin.get(ctx.db, 1)
      end
    end

    test "returns correct membership status", ctx do
      refute Goblin.has_key?(ctx.db, :key)
      Goblin.put(ctx.db, :key, :val)
      assert Goblin.has_key?(ctx.db, :key)

      refute Goblin.has_key?(ctx.db, :key, tag: :ns)
      Goblin.put(ctx.db, :key, :val, tag: :ns)
      assert Goblin.has_key?(ctx.db, :key, tag: :ns)
    end

    test "returns correct membership from disk", ctx do
      Goblin.put(ctx.db, :key, :val)
      trigger_flush(ctx.db)
      assert :ok == wait_until_idle(ctx.db)

      assert Goblin.has_key?(ctx.db, :key)
    end
  end

  describe "export/2" do
    setup ctx do
      export_dir = Path.join(ctx.tmp_dir, "exports")
      unpack_dir = Path.join(ctx.tmp_dir, "unpack")
      File.mkdir!(export_dir)
      File.mkdir!(unpack_dir)
      %{db: start_supervised_db(ctx), export_dir: export_dir, unpack_dir: unpack_dir}
    end

    test "exports snapshot of database", ctx do
      Goblin.put(ctx.db, :k1, :v1)
      Goblin.put(ctx.db, :k2, :v2)

      assert {:ok, tar_name} = Goblin.export(ctx.db, ctx.export_dir)

      :ok = :erl_tar.extract(~c"#{tar_name}", [:compressed, cwd: ~c"#{ctx.unpack_dir}"])

      Goblin.remove(ctx.db, :k2)

      assert {:ok, backup_db} =
               Goblin.start_link(name: Goblin.Backup, data_dir: ctx.unpack_dir)

      assert :v1 == Goblin.get(backup_db, :k1)
      assert :v2 == Goblin.get(backup_db, :k2)

      assert :v1 == Goblin.get(ctx.db, :k1)
      assert nil == Goblin.get(ctx.db, :k2)
    end
  end

  describe "flush and compaction" do
    setup ctx, do: %{db: start_supervised_db(ctx)}

    test "data survives flush to disk", ctx do
      pairs = for n <- 1..200, do: {:"key_#{n}", "value_#{n}"}
      Goblin.put_multi(ctx.db, pairs)

      trigger_flush(ctx.db)
      assert :ok == wait_until_idle(ctx.db)

      sst_files = Path.wildcard(Path.join(ctx.tmp_dir, "*.goblin"))
      assert length(sst_files) > 0

      for {key, val} <- pairs do
        assert val == Goblin.get(ctx.db, key)
      end

      keys = Enum.map(pairs, &elem(&1, 0))
      result = Goblin.get_multi(ctx.db, keys)
      assert length(result) == length(pairs)

      scan_result = Goblin.scan(ctx.db) |> Enum.to_list()

      for pair <- pairs do
        assert pair in scan_result
      end
    end

    test "data survives compaction", ctx do
      all_pairs =
        for wave <- 1..5, reduce: [] do
          acc ->
            pairs = for n <- 1..50, do: {:"w#{wave}_k#{n}", "w#{wave}_v#{n}"}
            Goblin.put_multi(ctx.db, pairs)
            trigger_flush(ctx.db)
            assert :ok == wait_until_idle(ctx.db)
            acc ++ pairs
        end

      Goblin.put(ctx.db, :shared_key, :old_value)
      trigger_flush(ctx.db)
      assert :ok == wait_until_idle(ctx.db)

      Goblin.put(ctx.db, :shared_key, :new_value)
      trigger_flush(ctx.db)
      assert :ok == wait_until_idle(ctx.db)

      assert :new_value == Goblin.get(ctx.db, :shared_key)

      for {key, val} <- all_pairs do
        assert val == Goblin.get(ctx.db, key)
      end
    end

    test "data survives flush, compaction, and restart", ctx do
      pairs =
        for wave <- 1..5, reduce: [] do
          acc ->
            pairs = for n <- 1..50, do: {:"w#{wave}_k#{n}", "w#{wave}_v#{n}"}
            Goblin.put_multi(ctx.db, pairs)
            trigger_flush(ctx.db)
            assert :ok == wait_until_idle(ctx.db)
            acc ++ pairs
        end

      Goblin.put(ctx.db, :deleted_key, :should_be_gone)
      Goblin.remove(ctx.db, :deleted_key)
      trigger_flush(ctx.db)
      assert :ok == wait_until_idle(ctx.db)

      stop_supervised(__MODULE__)
      {:ok, db} = start_supervised({Goblin, [data_dir: ctx.tmp_dir] ++ @default_opts})

      # check if ready
      _ = Goblin.flushing?(db)

      for {key, val} <- pairs do
        assert val == Goblin.get(db, key)
      end

      assert nil == Goblin.get(db, :deleted_key)
    end
  end

  describe "crash recovery" do
    test "committed data is recovered after process kill", ctx do
      db = start_db(ctx)

      Goblin.put(db, :key1, "value1")
      Goblin.put(db, :key2, "value2")

      assert "value1" == Goblin.get(db, :key1)
      assert "value2" == Goblin.get(db, :key2)

      kill_db(db)
      db = start_db(ctx)

      assert "value1" == Goblin.get(db, :key1)
      assert "value2" == Goblin.get(db, :key2)

      Goblin.stop(db)
    end

    test "both flushed and unflushed data survive a kill", ctx do
      db = start_db(ctx)

      # Write enough to trigger a flush
      pairs_wave1 = for n <- 1..100, do: {:"flushed_#{n}", "val_#{n}"}
      Goblin.put_multi(db, pairs_wave1)
      trigger_flush(db)
      assert :ok == wait_until_idle(db)

      # Write more data that stays in the WAL (not yet flushed)
      Goblin.put(db, :unflushed_key, "unflushed_val")

      kill_db(db)
      db = start_db(ctx)

      # Both flushed (SST) and unflushed (WAL) data should be present
      for {key, val} <- pairs_wave1 do
        assert val == Goblin.get(db, key)
      end

      assert "unflushed_val" == Goblin.get(db, :unflushed_key)

      Goblin.stop(db)
    end

    test "removes are recovered after kill", ctx do
      db = start_db(ctx)

      Goblin.put(db, :alive, "yes")
      Goblin.put(db, :dead, "soon")
      Goblin.remove(db, :dead)

      kill_db(db)
      db = start_db(ctx)

      assert "yes" == Goblin.get(db, :alive)
      assert nil == Goblin.get(db, :dead)

      Goblin.stop(db)
    end

    test "data accumulates correctly across multiple crash/restart cycles", ctx do
      # Use a mem_limit large enough that each cycle's puts fit in a single WAL
      # without rotation thrash. The 2KiB default forces a rotation on every
      # put because ets's baseline already exceeds it.
      opts = [mem_limit: 64 * 1024]
      start = fn -> start_db(ctx, opts) end

      db =
        Enum.reduce(1..5, start.(), fn cycle, db ->
          # Write data in this cycle
          for i <- 1..5 do
            Goblin.put(db, :"cycle_#{cycle}_key_#{i}", "cycle_#{cycle}_val_#{i}")
          end

          # Kill and restart
          kill_db(db)
          db = start.()

          # Verify ALL data from all previous cycles survived
          for past_cycle <- 1..cycle, past_i <- 1..5 do
            assert "cycle_#{past_cycle}_val_#{past_i}" ==
                     Goblin.get(db, :"cycle_#{past_cycle}_key_#{past_i}"),
                   "Missing data from cycle #{past_cycle}, key #{past_i} after crash cycle #{cycle}"
          end

          db
        end)

      Goblin.stop(db)
    end

    test "cannot have nested transactions", ctx do
      db = start_supervised_db(ctx)

      assert_raise RuntimeError, "Cannot start a transaction from within a transaction", fn ->
        Goblin.transaction(db, fn _tx ->
          Goblin.put(db, :key, :val)
        end)
      end

      assert :ok == Goblin.put(db, :key, :val)
      assert :val == Goblin.get(db, :key)
    end

    test "orphan files are removed during start", ctx do
      orphan_path = Path.join(ctx.tmp_dir, "orphan.goblin")
      File.touch(orphan_path)
      start_supervised_db(ctx)
      refute File.exists?(orphan_path)
    end
  end

  defp start_db(ctx, overrides \\ []) do
    opts = Keyword.merge(@default_opts, overrides)
    {:ok, db} = Goblin.start([name: ctx.test, data_dir: ctx.tmp_dir] ++ opts)
    db
  end

  defp start_supervised_db(ctx) do
    start_supervised!({Goblin, [name: ctx.test, data_dir: ctx.tmp_dir] ++ @default_opts},
      id: __MODULE__
    )
  end

  defp kill_db(db) do
    ref = Process.monitor(db)
    Process.exit(db, :kill)
    assert_receive {:DOWN, ^ref, :process, ^db, :killed}, 5000
    # allow cleanup to complete
    Process.sleep(50)
  end

  defp trigger_flush(db) do
    Stream.iterate(1, &(&1 + 1))
    |> Stream.each(fn x -> Goblin.put(db, x, "val-#{x}") end)
    |> Stream.drop_while(fn _ -> not Goblin.flushing?(db) end)
    |> Enum.take(1)
  end

  defp wait_until_idle(db, timeout \\ 2000, step \\ 50)
  defp wait_until_idle(_db, timeout, _step) when timeout <= 0, do: :timeout

  defp wait_until_idle(db, timeout, step) do
    if Goblin.flushing?(db) or Goblin.compacting?(db) do
      Process.sleep(step)
      wait_until_idle(db, timeout - step, step)
    else
      :ok
    end
  end

  defp operation_gen do
    one_of([
      tuple({constant(:put), term(), term()}),
      tuple({constant(:remove), term()})
    ])
  end
end
