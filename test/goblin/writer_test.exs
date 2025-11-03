defmodule Goblin.WriterTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog
  alias Goblin.Writer
  alias Goblin.WAL

  defmodule FakeTask do
    def async(_sup, _f), do: %{ref: make_ref()}
  end

  defmodule FailingTask do
    def async(_sup, _f) do
      Task.async(fn -> {:error, :failed_to_flush} end)
    end
  end

  @moduletag :tmp_dir
  @db_opts [
    id: :writer_test,
    name: __MODULE__,
    key_limit: 10,
    sync_interval: 50,
    wal_name: :writer_test_wal,
    manifest_name: :writer_test_manifest
  ]

  setup_db(@db_opts)

  test "writer starts with empty write state", c do
    assert %{seq: 0, mem_table: mem_table} = :sys.get_state(c.writer)
    assert %{} == mem_table
  end

  test "writes increase sequence", c do
    assert %{seq: 0} = :sys.get_state(c.writer)
    assert :ok == Writer.put(c.registry, :k1, :v1)
    assert :ok == Writer.put(c.registry, :k2, :v2)
    assert :ok == Writer.remove(c.registry, :k3)
    assert %{seq: 3} = :sys.get_state(c.writer)
  end

  test "writer can put key-value in memory", c do
    assert :ok == Writer.put(c.registry, :k1, :v1)
    assert :ok == Writer.put(c.registry, :k2, :v2)
    assert :ok == Writer.put(c.registry, :k3, :v3)
    assert {:ok, {:value, 0, :v1}} == Writer.get(c.registry, :k1)
    assert {:ok, {:value, 1, :v2}} == Writer.get(c.registry, :k2)
    assert {:ok, {:value, 2, :v3}} == Writer.get(c.registry, :k3)
  end

  test "writer can remove key-value pairs", c do
    assert :ok == Writer.remove(c.registry, :k)
    assert {:ok, {:value, 0, :tombstone}} == Writer.get(c.registry, :k)
    assert :ok == Writer.put(c.registry, :k, :v)
    assert {:ok, {:value, 1, :v}} == Writer.get(c.registry, :k)
    assert :ok == Writer.remove(c.registry, :k)
    assert {:ok, {:value, 2, :tombstone}} == Writer.get(c.registry, :k)
  end

  test "writer recovers memory after restart", c do
    assert :ok == Writer.put(c.registry, :k1, :v1)
    assert :ok == Writer.put(c.registry, :k2, :v2)
    assert :ok == Writer.remove(c.registry, :k3)
    WAL.sync(c.registry)
    stop_supervised!(c.db_id)
    start_db(c.tmp_dir, @db_opts)
    assert {:ok, {:value, 0, :v1}} == Writer.get(c.registry, :k1)
    assert {:ok, {:value, 1, :v2}} == Writer.get(c.registry, :k2)
    assert {:ok, {:value, 2, :tombstone}} == Writer.get(c.registry, :k3)
  end

  test "overflowing the MemTable causes a flush and writes data to disk", c do
    assert %{seq: 0, flushing: []} = :sys.get_state(c.writer)
    no_of_files = length(File.ls!(c.tmp_dir))

    for n <- 1..10 do
      assert :ok == Writer.put(c.registry, n, "v-#{n}")
    end

    assert_eventually do
      assert %{seq: 10, flushing: []} = :sys.get_state(c.writer)
    end

    assert no_of_files + 1 == length(File.ls!(c.tmp_dir))
  end

  @tag db_opts: [task_mod: FakeTask]
  test "recovers flushes after restart", c do
    for n <- 1..20 do
      assert :ok == Writer.put(c.registry, n, "v-#{n}")
    end

    assert %{
             flushing: [
               {_, mem_table1, rotated_wal1, _},
               {_, mem_table2, rotated_wal2, _}
             ]
           } = :sys.get_state(c.writer)

    WAL.sync(c.registry)
    stop_supervised!(c.db_id)
    %{writer: writer} = start_db(c.tmp_dir, @db_opts)

    assert %{
             flushing: [
               {_, ^mem_table1, ^rotated_wal1, _},
               {_, ^mem_table2, ^rotated_wal2, _}
             ]
           } = :sys.get_state(writer)
  end

  @tag db_opts: [task_mod: FakeTask]
  test "able to read data inbetween flush and flushed", c do
    for n <- 1..10 do
      assert :ok == Writer.put(c.registry, n, "v-#{n}")
    end

    assert %{mem_table: mem_table, flushing: [{_, _, _, _}]} = :sys.get_state(c.writer)
    assert %{} == mem_table

    for n <- 1..10 do
      assert {:ok, {:value, n - 1, "v-#{n}"}} == Writer.get(c.registry, n)
    end
  end

  @tag db_opts: [task_mod: FakeTask]
  test "able to read data from flushing MemTables", c do
    for n <- 1..30 do
      assert :ok == Writer.put(c.registry, n, "v-#{n}")
    end

    assert %{flushing: [{_, _, _, _}, {_, _, _, _}, {_, _, _, _}]} = :sys.get_state(c.writer)

    for n <- 1..30 do
      assert {:ok, {:value, n - 1, "v-#{n}"}} == Writer.get(c.registry, n)
    end
  end

  @tag db_opts: [task_mod: FakeTask]
  test "reads from latest write", c do
    for i <- 1..3 do
      for n <- 1..10 do
        assert :ok == Writer.put(c.registry, n, "v#{i}-#{n}")
      end
    end

    for n <- 1..10 do
      assert {:ok, {:value, n + 20 - 1, "v3-#{n}"}} == Writer.get(c.registry, n)
    end
  end

  @tag db_opts: [task_mod: FakeTask]
  test "get_iterators/1 gets iterator over current and flushing MemTables", c do
    for i <- 1..3 do
      for n <- 1..10 do
        assert :ok == Writer.put(c.registry, n, "v#{i}-#{n}")
      end
    end

    assert iterators = Writer.get_iterators(c.registry)
    assert length(iterators) == 4
  end

  @tag db_opts: [task_mod: FailingTask]
  test "failing flushes are retried until Writer exits", c do
    Process.flag(:trap_exit, true)

    log =
      capture_log(fn ->
        for n <- 1..10 do
          assert :ok == Writer.put(c.registry, n, "v-#{n}")
        end

        assert_receive {:EXIT, _pid, :shutdown}
      end)

    assert log =~ "Flush failed with reason: :failed_to_flush. Retrying..."
    assert log =~ "Flush failed after 5 attempts with reason: :failed_to_flush. Exiting."
  end

  test "merges MemTable with transactions writes after commit", c do
    assert :ok == Writer.put(c.registry, :i, :u)

    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               tx = Writer.Transaction.put(tx, :k, :v)
               tx = Writer.Transaction.put(tx, :l, :w)
               tx = Writer.Transaction.remove(tx, :m)
               assert {nil, tx} = Writer.Transaction.get(tx, :n)
               assert {:u, tx} = Writer.Transaction.get(tx, :i)
               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 0, :u}} == Writer.get(c.registry, :i)
    assert {:ok, {:value, 1, :v}} == Writer.get(c.registry, :k)
    assert {:ok, {:value, 2, :w}} == Writer.get(c.registry, :l)
    assert {:ok, {:value, 3, :tombstone}} == Writer.get(c.registry, :m)
  end

  test "does not merge with MemTable after a transaction cancels", c do
    assert :ok == Writer.put(c.registry, :i, :u)

    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               tx = Writer.Transaction.put(tx, :k, :v)
               tx = Writer.Transaction.put(tx, :l, :w)
               tx = Writer.Transaction.remove(tx, :m)
               assert {nil, _tx} = Writer.Transaction.get(tx, :n)
               :cancel
             end)

    assert {:ok, {:value, 0, :u}} == Writer.get(c.registry, :i)
    assert :not_found == Writer.get(c.registry, :k)
    assert :not_found == Writer.get(c.registry, :l)
    assert :not_found == Writer.get(c.registry, :m)
  end

  test "concurrent non-conflicting transactions are merged into the MemTable", c do
    pid1 =
      spawn(fn ->
        pid2 =
          Writer.transaction(c.registry, fn tx ->
            tx = Writer.Transaction.put(tx, :k1, :v1)

            pid2 =
              receive do
                {:cont, pid2} -> pid2
              end

            {:commit, tx, pid2}
          end)

        send(pid2, :cont)
      end)

    spawn(fn ->
      Writer.transaction(c.registry, fn tx ->
        tx = Writer.Transaction.put(tx, :k2, :v2)

        send(pid1, {:cont, self()})

        receive do
          :cont -> :ok
        end

        {:commit, tx, :ok}
      end)
    end)

    assert_eventually do
      assert {:ok, {:value, 0, :v1}} == Writer.get(c.registry, :k1)
      assert {:ok, {:value, 1, :v2}} == Writer.get(c.registry, :k2)
    end
  end

  test "concurrent transactions with read-conflict are not all merged into the MemTable", c do
    pid1 =
      spawn_link(fn ->
        pid2 =
          Writer.transaction(c.registry, fn tx ->
            tx = Writer.Transaction.put(tx, :k1, :v1)

            pid2 =
              receive do
                {:cont, pid2} -> pid2
              end

            {:commit, tx, pid2}
          end)

        send(pid2, :cont)
      end)

    spawn(fn ->
      assert {:error, :tx_in_conflict} ==
               Writer.transaction(c.registry, fn tx ->
                 tx = Writer.Transaction.put(tx, :k2, :v2)
                 assert {nil, tx} = Writer.Transaction.get(tx, :k1)
                 send(pid1, {:cont, self()})

                 receive do
                   :cont -> :ok
                 end

                 {:commit, tx, :ok}
               end)
    end)

    assert_eventually do
      assert {:ok, {:value, 0, :v1}} == Writer.get(c.registry, :k1)
      assert :not_found == Writer.get(c.registry, :k2)
    end
  end

  test "concurrent transactions with write-conflicts are not all merged to the MemTable", c do
    pid1 =
      spawn(fn ->
        pid2 =
          Writer.transaction(c.registry, fn tx ->
            tx = Writer.Transaction.put(tx, :k1, :v1)

            pid2 =
              receive do
                {:cont, pid2} -> pid2
              end

            {:commit, tx, pid2}
          end)

        send(pid2, :cont)
      end)

    spawn(fn ->
      assert {:error, :tx_in_conflict} ==
               Writer.transaction(c.registry, fn tx ->
                 tx = Writer.Transaction.put(tx, :k1, :v2)
                 send(pid1, {:cont, self()})

                 receive do
                   :cont -> :ok
                 end

                 {:commit, tx, :ok}
               end)
    end)

    assert_eventually do
      assert {:ok, {:value, 0, :v1}} == Writer.get(c.registry, :k1)
    end
  end

  test "a transaction raises if it returns anything other than {:commit, tx, reply} or :cancel",
       c do
    assert_raise RuntimeError, fn ->
      Writer.transaction(c.registry, fn tx ->
        Writer.Transaction.put(tx, :k, :v)
        :tx_done
      end)
    end

    assert_eventually do
      assert :not_found == Writer.get(c.registry, :k1)
    end
  end

  test "all writes are appended to the WAL", c do
    assert :ok == Writer.put(c.registry, :k, :v)

    assert_eventually do
      assert {:ok, [{nil, [{0, :put, :k, :v}]}]} == WAL.recover(c.registry)
    end

    assert :ok == Writer.remove(c.registry, :k)

    assert_eventually do
      assert {:ok, [{nil, [{0, :put, :k, :v}, {1, :remove, :k}]}]} == WAL.recover(c.registry)
    end

    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               tx = Writer.Transaction.put(tx, :l, :w)
               tx = Writer.Transaction.remove(tx, :l)
               {:commit, tx, :ok}
             end)

    assert_eventually do
      assert {:ok,
              [{nil, [{0, :put, :k, :v}, {1, :remove, :k}, {2, :put, :l, :w}, {3, :remove, :l}]}]} ==
               WAL.recover(c.registry)
    end
  end

  test "cannot be in two transactions simultaneously", c do
    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               assert {:error, :already_in_transaction} ==
                        Writer.transaction(c.registry, fn tx ->
                          {:commit, tx, :ok}
                        end)

               {:commit, tx, :ok}
             end)
  end

  test "transactions are not persisted on restarts", c do
    parent = self()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()

    pid =
      spawn(fn ->
        assert {:error, :tx_not_found} ==
                 Writer.transaction(c.registry, fn tx ->
                   tx = Writer.Transaction.put(tx, :k, :v)
                   send(parent, {:ok, ref1})

                   receive do
                     {:ok, ^ref2} -> :ok
                   end

                   {:commit, tx, :ok}
                 end)

        send(parent, {:ok, ref3})
      end)

    receive do
      {:ok, ^ref1} -> :ok
    end

    stop_supervised!(c.db_id)
    start_db(c.tmp_dir, @db_opts)

    send(pid, {:ok, ref2})

    assert_receive {:ok, ^ref3}

    assert :not_found == Writer.get(c.registry, :k)
  end

  test "get returns :not_found for non-existent key", c do
    assert :not_found == Writer.get(c.registry, :non_existent)
  end

  test "is_flushing returns false when not flushing", c do
    refute Writer.is_flushing(c.registry)
  end

  @tag db_opts: [task_mod: FakeTask]
  test "is_flushing returns true when flushing", c do
    for n <- 1..10 do
      assert :ok == Writer.put(c.registry, n, "v-#{n}")
    end

    assert Writer.is_flushing(c.registry)
  end

  test "empty transaction commits successfully", c do
    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               {:commit, tx, :ok}
             end)

    assert %{seq: 0} = :sys.get_state(c.writer)
  end

  test "transaction with remove then put on same key", c do
    assert :ok == Writer.put(c.registry, :k, :v1)

    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               tx = Writer.Transaction.remove(tx, :k)
               tx = Writer.Transaction.put(tx, :k, :v2)
               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 2, :v2}} == Writer.get(c.registry, :k)
  end

  test "transaction with put then remove on same key", c do
    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               tx = Writer.Transaction.put(tx, :k, :v)
               tx = Writer.Transaction.remove(tx, :k)
               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 1, :tombstone}} == Writer.get(c.registry, :k)
  end

  test "multiple sequential flushes maintain correct state", c do
    for batch <- 1..3 do
      for n <- 1..10 do
        key = "batch#{batch}_key#{n}"
        assert :ok == Writer.put(c.registry, key, "value#{n}")
      end

      assert_eventually do
        refute Writer.is_flushing(c.registry)
      end
    end

    assert %{seq: 30, flushing: []} = :sys.get_state(c.writer)
  end

  test "transaction can read its own writes", c do
    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               tx = Writer.Transaction.put(tx, :k1, :v1)
               assert {:v1, tx} = Writer.Transaction.get(tx, :k1)

               tx = Writer.Transaction.put(tx, :k1, :v2)
               assert {:v2, tx} = Writer.Transaction.get(tx, :k1)

               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 1, :v2}} == Writer.get(c.registry, :k1)
  end

  test "transaction reads from writer when key not in transaction", c do
    assert :ok == Writer.put(c.registry, :k, :v)

    assert :ok ==
             Writer.transaction(c.registry, fn tx ->
               assert {:v, tx} = Writer.Transaction.get(tx, :k)
               {:commit, tx, :ok}
             end)
  end

  test "transactions are retried on conflicts", c do
    parent = self()

    pid =
      spawn(fn ->
        assert {:error, :tx_in_conflict} ==
                 Writer.transaction(
                   c.registry,
                   fn tx ->
                     send(parent, :ready)

                     receive do
                       :cont -> :ok
                     end

                     tx = Writer.Transaction.put(tx, :k, :v)
                     {:commit, tx, :ok}
                   end,
                   retries: 1
                 )
      end)

    assert_receive :ready

    Writer.put(c.registry, :k, :w)

    send(pid, :cont)

    assert_receive :ready

    assert {:ok, {:value, 0, :w}} == Writer.get(c.registry, :k)
  end

  test "transactions can time out", c do
    assert {:error, :tx_timed_out} ==
             Writer.transaction(
               c.registry,
               fn tx ->
                 tx = Writer.Transaction.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end,
               timeout: 0
             )

    assert :not_found == Writer.get(c.registry, :k)

    assert :ok ==
             Writer.transaction(
               c.registry,
               fn tx ->
                 tx = Writer.Transaction.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end,
               timeout: 200
             )

    assert {:ok, {:value, 0, :v}} == Writer.get(c.registry, :k)
  end

  test "transactions have snapshot isolation", c do
    parent = self()

    pid =
      spawn(fn ->
        assert :ok ==
                 Writer.transaction(c.registry, fn tx ->
                   send(parent, :ready)

                   receive do
                     :cont -> :ok
                   end

                   assert {nil, tx} = Writer.Transaction.get(tx, :k)
                   {:commit, tx, :ok}
                 end)

        send(parent, :done)
      end)

    assert_receive :ready

    assert :ok == Writer.put(c.registry, :k, :v)

    send(pid, :cont)

    assert_receive :done
  end

  test "transactions have snapshot isolation in store", c do
    parent = self()

    for n <- 1..10 do
      assert :ok == Writer.put(c.registry, n, "v-#{n}")
    end

    pid =
      spawn(fn ->
        assert :ok ==
                 Writer.transaction(c.registry, fn tx ->
                   send(parent, :ready)

                   receive do
                     :cont -> :ok
                   end

                   assert {"v-5", tx} = Writer.Transaction.get(tx, 5)
                   {:commit, tx, :ok}
                 end)

        send(parent, :done)
      end)

    assert_receive :ready

    assert :ok == Writer.put(c.registry, 5, "w-5")

    send(pid, :cont)

    assert_receive :done
  end

  test "commits are published to subscribers", c do
    spawn(fn ->
      Goblin.subscribe(c.db)

      assert_receive {:put, :k, :v}, 500
      assert_receive {:remove, :l}, 500
      assert_receive {:put, :k1, :v1}, 500
      assert_receive {:put, :k2, :v2}, 500
      assert_receive {:put, :k3, :v3}, 500
      assert_receive {:remove, :l1}, 500
      assert_receive {:remove, :l2}, 500
      assert_receive {:remove, :l3}, 500
    end)

    spawn(fn ->
      Goblin.subscribe(c.db)

      assert_receive {:put, :k, :v}, 500
      assert_receive {:remove, :l}, 500

      Goblin.unsubscribe(c.db)

      refute_receive {:put, :k1, :v1}, 500
      refute_receive {:put, :k2, :v2}, 500
      refute_receive {:put, :k3, :v3}, 500
      refute_receive {:remove, :l1}, 500
      refute_receive {:remove, :l2}, 500
      refute_receive {:remove, :l3}, 500
    end)

    assert :ok == Writer.put(c.registry, :k, :v)
    assert :ok == Writer.remove(c.registry, :l)

    spawn(fn ->
      Goblin.subscribe(c.db)

      assert_receive {:put, :k1, :v1}, 500
      assert_receive {:put, :k2, :v2}, 500
      assert_receive {:put, :k3, :v3}, 500
      assert_receive {:remove, :l1}, 500
      assert_receive {:remove, :l2}, 500
      assert_receive {:remove, :l3}, 500
    end)

    assert :ok == Writer.put_multi(c.registry, k1: :v1, k2: :v2, k3: :v3)
    assert :ok == Writer.remove_multi(c.registry, [:l1, :l2, :l3])
  end
end
