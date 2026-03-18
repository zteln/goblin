defmodule GoblinTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @moduletag :tmp_dir

  @default_opts [mem_limit: 2 * 1024, bf_bit_array_size: 1000]

  setup c do
    db =
      start_supervised!(
        {Goblin, [data_dir: c.tmp_dir] ++ @default_opts},
        id: __MODULE__
      )

    %{db: db}
  end

  describe "start_link/1" do
    test "can start multiple independent databases simultaneously", c do
      dir1 = Path.join(c.tmp_dir, "dir1")
      dir2 = Path.join(c.tmp_dir, "dir2")
      dir3 = Path.join(c.tmp_dir, "dir3")

      assert {:ok, db1} = Goblin.start_link(name: DB1, data_dir: dir1)
      assert {:ok, db2} = Goblin.start_link(name: DB2, data_dir: dir2)
      assert {:ok, db3} = Goblin.start_link(name: DB3, data_dir: dir3)

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
      assert :ok == Goblin.stop(c.db)
      refute Process.alive?(c.db)
    end
  end

  describe "atomicity" do
    test "writes are only visible after commit", c do
      parent = self()

      assert nil == Goblin.get(c.db, :key1)
      assert nil == Goblin.get(c.db, :key2)
      assert nil == Goblin.get(c.db, :key3)
      assert [] == Goblin.get_multi(c.db, [:key1, :key2, :key3])
      assert [] == Goblin.scan(c.db) |> Enum.to_list()

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
      assert [] == Goblin.scan(c.db) |> Enum.to_list()

      send(writer, :cont)

      assert_receive :done

      assert :val1 == Goblin.get(c.db, :key1)
      assert :val2 == Goblin.get(c.db, :key2)
      assert nil == Goblin.get(c.db, :key3)
      assert [{:key1, :val1}, {:key2, :val2}] == Goblin.get_multi(c.db, [:key1, :key2, :key3])
      assert [{:key1, :val1}, {:key2, :val2}] == Goblin.scan(c.db) |> Enum.to_list()
    end

    test "aborted commits cannot be read", c do
      assert {:error, :aborted} ==
               Goblin.transaction(c.db, fn tx ->
                 Goblin.Tx.put(tx, :key, :val)
                 :abort
               end)

      assert nil == Goblin.get(c.db, :key)
    end

    test "exception in callback releases write lock", c do
      assert_raise RuntimeError, "boom", fn ->
        Goblin.transaction(c.db, fn tx ->
          Goblin.Tx.put(tx, :key, :val)
          raise "boom"
        end)
      end

      assert nil == Goblin.get(c.db, :key)

      # Verify the database is still functional
      assert :ok == Goblin.put(c.db, :after_crash, :works)
      assert :works == Goblin.get(c.db, :after_crash)
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
    test "can maintain a business rule", c do
      Goblin.put(c.db, :counter, 10)

      results =
        for _n <- 1..20 do
          Task.async(fn ->
            Goblin.transaction(c.db, fn tx ->
              counter = Goblin.Tx.get(tx, :counter, default: 0)

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

  describe "queries" do
    test "scan respects min/max bounds", c do
      Goblin.put_multi(c.db, [{1, :a}, {2, :b}, {3, :c}, {4, :d}, {5, :e}])

      assert [{2, :b}, {3, :c}, {4, :d}] == Goblin.scan(c.db, min: 2, max: 4) |> Enum.to_list()
      assert [{3, :c}, {4, :d}, {5, :e}] == Goblin.scan(c.db, min: 3) |> Enum.to_list()
      assert [{1, :a}, {2, :b}] == Goblin.scan(c.db, max: 2) |> Enum.to_list()

      assert [{1, :a}, {2, :b}, {3, :c}, {4, :d}, {5, :e}] ==
               Goblin.scan(c.db) |> Enum.to_list()
    end

    test "remove then put overwrites tombstone", c do
      Goblin.put(c.db, :key, :first)
      assert :first == Goblin.get(c.db, :key)

      Goblin.remove(c.db, :key)
      assert nil == Goblin.get(c.db, :key)
      assert [] == Goblin.scan(c.db) |> Enum.to_list()

      Goblin.put(c.db, :key, :second)
      assert :second == Goblin.get(c.db, :key)
      assert [{:key, :second}] == Goblin.scan(c.db) |> Enum.to_list()
    end

    test "remove_multi removes multiple keys", c do
      Goblin.put_multi(c.db, [{:a, 1}, {:b, 2}, {:c, 3}, {:d, 4}])

      Goblin.remove_multi(c.db, [:b, :c])

      assert 1 == Goblin.get(c.db, :a)
      assert nil == Goblin.get(c.db, :b)
      assert nil == Goblin.get(c.db, :c)
      assert 4 == Goblin.get(c.db, :d)

      assert [{:a, 1}, {:d, 4}] == Goblin.scan(c.db) |> Enum.to_list()
    end

    test "get returns default when key is missing", c do
      assert nil == Goblin.get(c.db, :missing)
      assert :fallback == Goblin.get(c.db, :missing, default: :fallback)

      Goblin.put(c.db, :exists, :val)
      assert :val == Goblin.get(c.db, :exists, default: :fallback)
    end

    test "scan respects tag with min/max bounds", c do
      for n <- 1..5 do
        Goblin.put(c.db, n, "users_#{n}", tag: :users)
        Goblin.put(c.db, n, "orders_#{n}", tag: :orders)
      end

      Goblin.put_multi(c.db, [{1, "untagged_1"}, {2, "untagged_2"}])

      # tag + min + max
      result = Goblin.scan(c.db, tag: :users, min: 2, max: 4) |> Enum.to_list()
      assert [{2, "users_2"}, {3, "users_3"}, {4, "users_4"}] == result

      # tag + min only
      result = Goblin.scan(c.db, tag: :users, min: 4) |> Enum.to_list()
      assert [{4, "users_4"}, {5, "users_5"}] == result

      # tag + max only
      result = Goblin.scan(c.db, tag: :users, max: 2) |> Enum.to_list()
      assert [{1, "users_1"}, {2, "users_2"}] == result

      # tag without bounds returns all entries for that tag
      result = Goblin.scan(c.db, tag: :orders) |> Enum.to_list()
      assert length(result) == 5
    end

    test "read transaction supports tags", c do
      Goblin.put(c.db, :key, :tagged_val, tag: :ns)
      Goblin.put(c.db, :key, :plain_val)

      Goblin.read(c.db, fn tx ->
        assert :tagged_val == Goblin.Tx.get(tx, :key, tag: :ns)
        assert :plain_val == Goblin.Tx.get(tx, :key)
        assert nil == Goblin.Tx.get(tx, :key, tag: :other)

        assert [{:key, :tagged_val}] == Goblin.Tx.get_multi(tx, [:key], tag: :ns)
        assert [{:key, :plain_val}] == Goblin.Tx.get_multi(tx, [:key])
      end)
    end
  end

  describe "isolated" do
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

      stop_supervised(__MODULE__)
      {:ok, db} = start_supervised({Goblin, [data_dir: c.tmp_dir] ++ @default_opts})

      assert :v == Goblin.get(db, :k)
      assert :w == Goblin.get(db, :l)
    end
  end

  describe "export/2" do
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
          assert {:ok, backup_db} = Goblin.start_link(name: Goblin.Backup, data_dir: c.unpack_dir)
          backup_db
        end)

      # Wait for handle_continue chains to complete before reading
      _ = Goblin.flushing?(backup_db)

      assert :v1 == Goblin.get(backup_db, :k1)
      assert :v2 == Goblin.get(backup_db, :k2)

      assert :v1 == Goblin.get(c.db, :k1)
      assert nil == Goblin.get(c.db, :k2)
    end
  end

  describe "property test" do
    test "can write and read any term as key or value", c do
      len = 100

      pairs =
        StreamData.term()
        |> Stream.chunk_every(2)
        |> Stream.map(fn [key, val] -> {key, val} end)
        |> Enum.take(len)

      output_pairs =
        pairs
        |> Enum.with_index(fn {key, val}, seq -> {key, seq, val} end)
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> Goblin.TestHelper.uniq_by_value(&elem(&1, 0))
        |> Enum.map(fn {key, _seq, val} -> {key, val} end)

      keys = Enum.map(output_pairs, &elem(&1, 0))

      assert :ok == Goblin.put_multi(c.db, pairs)

      assert output_pairs == Goblin.get_multi(c.db, keys)

      keys = Enum.map(output_pairs, &elem(&1, 0))

      assert output_pairs == Goblin.get_multi(c.db, keys)
    end

    test "tags can be any term", c do
      len = 100

      data =
        StreamData.term()
        |> Stream.chunk_every(3)
        |> Stream.map(fn
          [tag, key, val] -> {tag, key, val}
        end)
        |> Enum.take(len)

      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 data
                 |> Enum.reduce(tx, fn {tag, key, value}, acc ->
                   Goblin.Tx.put(acc, key, value, tag: tag)
                 end)
                 |> Goblin.Tx.commit()
               end)

      data =
        data
        |> Enum.with_index(fn {tag, key, val}, seq -> {tag, key, seq, val} end)
        |> Enum.sort_by(fn {_tag, key, seq, _val} -> {key, -seq} end)
        |> Goblin.TestHelper.uniq_by_value(&elem(&1, 1))
        |> Enum.map(fn {tag, key, _seq, val} -> {tag, key, val} end)

      data
      |> Enum.each(fn {tag, key, val} ->
        assert val == Goblin.get(c.db, key, tag: tag)
        assert [{key, val}] == Goblin.get_multi(c.db, [key], tag: tag)
        assert {key, val} in (Goblin.scan(c.db, tag: tag) |> Enum.to_list())
      end)
    end
  end

  describe "flush and compaction" do
    test "data survives flush to disk", c do
      pairs = for n <- 1..200, do: {:"key_#{n}", "value_#{n}"}
      Goblin.put_multi(c.db, pairs)

      trigger_flush(c.db)
      assert :ok == wait_until_idle(c.db)

      sst_files = Path.wildcard(Path.join(c.tmp_dir, "*.goblin"))
      assert length(sst_files) > 0

      for {key, val} <- pairs do
        assert val == Goblin.get(c.db, key)
      end

      keys = Enum.map(pairs, &elem(&1, 0))
      result = Goblin.get_multi(c.db, keys)
      assert length(result) == length(pairs)

      scan_result = Goblin.scan(c.db) |> Enum.to_list()

      for pair <- pairs do
        assert pair in scan_result
      end
    end

    test "data survives compaction", c do
      all_pairs =
        for wave <- 1..5, reduce: [] do
          acc ->
            pairs = for n <- 1..50, do: {:"w#{wave}_k#{n}", "w#{wave}_v#{n}"}
            Goblin.put_multi(c.db, pairs)
            trigger_flush(c.db)
            assert :ok == wait_until_idle(c.db)
            acc ++ pairs
        end

      Goblin.put(c.db, :shared_key, :old_value)
      trigger_flush(c.db)
      assert :ok == wait_until_idle(c.db)

      Goblin.put(c.db, :shared_key, :new_value)
      trigger_flush(c.db)
      assert :ok == wait_until_idle(c.db)

      assert :new_value == Goblin.get(c.db, :shared_key)

      for {key, val} <- all_pairs do
        assert val == Goblin.get(c.db, key)
      end
    end

    test "data survives flush, compaction, and restart", c do
      pairs =
        for wave <- 1..5, reduce: [] do
          acc ->
            pairs = for n <- 1..50, do: {:"w#{wave}_k#{n}", "w#{wave}_v#{n}"}
            Goblin.put_multi(c.db, pairs)
            trigger_flush(c.db)
            assert :ok == wait_until_idle(c.db)
            acc ++ pairs
        end

      Goblin.put(c.db, :deleted_key, :should_be_gone)
      Goblin.remove(c.db, :deleted_key)
      trigger_flush(c.db)
      assert :ok == wait_until_idle(c.db)

      stop_supervised(__MODULE__)
      {:ok, db} = start_supervised({Goblin, [data_dir: c.tmp_dir] ++ @default_opts})

      _ = Goblin.flushing?(db)

      for {key, val} <- pairs do
        assert val == Goblin.get(db, key)
      end

      assert nil == Goblin.get(db, :deleted_key)
    end
  end

  defp trigger_flush(db) do
    StreamData.term()
    |> Stream.chunk_every(2)
    |> Stream.map(fn [k, v] -> {k, v} end)
    |> Stream.each(fn {k, v} -> Goblin.put(db, k, v) end)
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
end
