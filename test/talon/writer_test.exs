defmodule Talon.WriterTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog
  alias Talon.Writer
  alias Talon.WAL

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
    name: :writer_test,
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
    assert :ok == Writer.put(c.writer, :k1, :v1)
    assert :ok == Writer.put(c.writer, :k2, :v2)
    assert :ok == Writer.remove(c.writer, :k3)
    assert %{seq: 3} = :sys.get_state(c.writer)
  end

  test "writer can put key-value in memory", c do
    assert :ok == Writer.put(c.writer, :k1, :v1)
    assert :ok == Writer.put(c.writer, :k2, :v2)
    assert :ok == Writer.put(c.writer, :k3, :v3)
    assert {:ok, {:value, 0, :v1}} == Writer.get(c.writer, :k1)
    assert {:ok, {:value, 1, :v2}} == Writer.get(c.writer, :k2)
    assert {:ok, {:value, 2, :v3}} == Writer.get(c.writer, :k3)
  end

  test "writer can remove key-value pairs", c do
    assert :ok == Writer.remove(c.writer, :k)
    assert {:ok, {:value, 0, nil}} == Writer.get(c.writer, :k)
    assert :ok == Writer.put(c.writer, :k, :v)
    assert {:ok, {:value, 1, :v}} == Writer.get(c.writer, :k)
    assert :ok == Writer.remove(c.writer, :k)
    assert {:ok, {:value, 2, nil}} == Writer.get(c.writer, :k)
  end

  test "writer recovers memory after restart", c do
    assert :ok == Writer.put(c.writer, :k1, :v1)
    assert :ok == Writer.put(c.writer, :k2, :v2)
    assert :ok == Writer.remove(c.writer, :k3)
    WAL.sync(c.wal)
    stop_supervised!(c.db_id)
    %{writer: writer} = start_db(c.tmp_dir, @db_opts)
    assert {:ok, {:value, 0, :v1}} == Writer.get(writer, :k1)
    assert {:ok, {:value, 1, :v2}} == Writer.get(writer, :k2)
    assert {:ok, {:value, 2, nil}} == Writer.get(writer, :k3)
  end

  test "overflowing the MemTable causes a flush and writes data to disk", c do
    assert %{seq: 0, flushing: []} = :sys.get_state(c.writer)
    no_of_files = length(File.ls!(c.tmp_dir))

    for n <- 1..10 do
      assert :ok == Writer.put(c.writer, n, "v-#{n}")
    end

    assert_eventually do
      assert %{seq: 10, flushing: []} = :sys.get_state(c.writer)
    end

    assert no_of_files + 1 == length(File.ls!(c.tmp_dir))
  end

  @tag db_opts: [task_mod: FakeTask]
  test "recovers flushes after restart", c do
    for n <- 1..20 do
      assert :ok == Writer.put(c.writer, n, "v-#{n}")
    end

    assert %{
             flushing: [
               {_, mem_table1, rotated_wal1, _},
               {_, mem_table2, rotated_wal2, _}
             ]
           } = :sys.get_state(c.writer)

    WAL.sync(c.wal)
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
      assert :ok == Writer.put(c.writer, n, "v-#{n}")
    end

    assert %{mem_table: mem_table, flushing: [{_, _, _, _}]} = :sys.get_state(c.writer)
    assert %{} == mem_table

    for n <- 1..10 do
      assert {:ok, {:value, n - 1, "v-#{n}"}} == Writer.get(c.writer, n)
    end
  end

  @tag db_opts: [task_mod: FakeTask]
  test "able to read data from flushing MemTables", c do
    for n <- 1..30 do
      assert :ok == Writer.put(c.writer, n, "v-#{n}")
    end

    assert %{flushing: [{_, _, _, _}, {_, _, _, _}, {_, _, _, _}]} = :sys.get_state(c.writer)

    for n <- 1..30 do
      assert {:ok, {:value, n - 1, "v-#{n}"}} == Writer.get(c.writer, n)
    end
  end

  @tag db_opts: [task_mod: FakeTask]
  test "reads from latest write", c do
    for i <- 1..3 do
      for n <- 1..10 do
        assert :ok == Writer.put(c.writer, n, "v#{i}-#{n}")
      end
    end

    for n <- 1..10 do
      assert {:ok, {:value, n + 20 - 1, "v3-#{n}"}} == Writer.get(c.writer, n)
    end
  end

  @tag db_opts: [task_mod: FailingTask]
  test "failing flushes are retried until Writer exits", c do
    Process.flag(:trap_exit, true)

    log =
      capture_log(fn ->
        for n <- 1..10 do
          assert :ok == Writer.put(c.writer, n, "v-#{n}")
        end

        assert_receive {:EXIT, _pid, :shutdown}
      end)

    assert log =~ "Flush failed with reason: :failed_to_flush. Retrying..."
    assert log =~ "Flush failed after 5 attempts with reason: :failed_to_flush. Exiting."
    assert log =~ "GenServer :writer_test_writer terminating"
  end

  test "merges MemTable with transactions writes after commit", c do
    assert :ok == Writer.put(c.writer, :i, :u)

    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               tx = Writer.Transaction.put(tx, :k, :v)
               tx = Writer.Transaction.put(tx, :l, :w)
               tx = Writer.Transaction.remove(tx, :m)
               assert {:not_found, tx} = Writer.Transaction.get(tx, :n)
               assert {{0, :u}, tx} = Writer.Transaction.get(tx, :i)
               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 0, :u}} == Writer.get(c.writer, :i)
    assert {:ok, {:value, 1, :v}} == Writer.get(c.writer, :k)
    assert {:ok, {:value, 2, :w}} == Writer.get(c.writer, :l)
    assert {:ok, {:value, 3, nil}} == Writer.get(c.writer, :m)
  end

  test "does not merge with MemTable after a transaction cancels", c do
    assert :ok == Writer.put(c.writer, :i, :u)

    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               tx = Writer.Transaction.put(tx, :k, :v)
               tx = Writer.Transaction.put(tx, :l, :w)
               tx = Writer.Transaction.remove(tx, :m)
               assert {:not_found, _tx} = Writer.Transaction.get(tx, :n)
               :cancel
             end)

    assert {:ok, {:value, 0, :u}} == Writer.get(c.writer, :i)
    assert :not_found == Writer.get(c.writer, :k)
    assert :not_found == Writer.get(c.writer, :l)
    assert :not_found == Writer.get(c.writer, :m)
  end

  test "concurrent non-conflicting transactions are merged into the MemTable", c do
    pid1 =
      spawn(fn ->
        pid2 =
          Writer.transaction(c.writer, fn tx ->
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
      Writer.transaction(c.writer, fn tx ->
        tx = Writer.Transaction.put(tx, :k2, :v2)

        send(pid1, {:cont, self()})

        receive do
          :cont -> :ok
        end

        {:commit, tx, :ok}
      end)
    end)

    assert_eventually do
      assert {:ok, {:value, 0, :v1}} == Writer.get(c.writer, :k1)
      assert {:ok, {:value, 1, :v2}} == Writer.get(c.writer, :k2)
    end
  end

  test "concurrent transactions with read-conflict are not all merged into the MemTable", c do
    pid1 =
      spawn_link(fn ->
        pid2 =
          Writer.transaction(c.writer, fn tx ->
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
      assert {:error, :in_conflict} ==
               Writer.transaction(c.writer, fn tx ->
                 tx = Writer.Transaction.put(tx, :k2, :v2)
                 assert {:not_found, tx} = Writer.Transaction.get(tx, :k1)
                 send(pid1, {:cont, self()})

                 receive do
                   :cont -> :ok
                 end

                 {:commit, tx, :ok}
               end)
    end)

    assert_eventually do
      assert {:ok, {:value, 0, :v1}} == Writer.get(c.writer, :k1)
      assert :not_found == Writer.get(c.writer, :k2)
    end
  end

  test "concurrent transactions with write-conflicts are not all merged to the MemTable", c do
    pid1 =
      spawn(fn ->
        pid2 =
          Writer.transaction(c.writer, fn tx ->
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
      assert {:error, :in_conflict} ==
               Writer.transaction(c.writer, fn tx ->
                 tx = Writer.Transaction.put(tx, :k1, :v2)
                 send(pid1, {:cont, self()})

                 receive do
                   :cont -> :ok
                 end

                 {:commit, tx, :ok}
               end)
    end)

    assert_eventually do
      assert {:ok, {:value, 0, :v1}} == Writer.get(c.writer, :k1)
    end
  end

  test "a transaction raises if it returns anything other than {:commit, tx, reply} or :cancel",
       c do
    assert_raise RuntimeError, fn ->
      Writer.transaction(c.writer, fn tx ->
        Writer.Transaction.put(tx, :k, :v)
        :tx_done
      end)
    end

    assert_eventually do
      assert :not_found == Writer.get(c.writer, :k1)
    end
  end

  test "all writes are appended to the WAL", c do
    assert :ok == Writer.put(c.writer, :k, :v)

    assert_eventually do
      assert {:ok, [{nil, [{0, :put, :k, :v}]}]} == WAL.recover(c.wal)
    end

    assert :ok == Writer.remove(c.writer, :k)

    assert_eventually do
      assert {:ok, [{nil, [{0, :put, :k, :v}, {1, :remove, :k}]}]} == WAL.recover(c.wal)
    end

    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               tx = Writer.Transaction.put(tx, :l, :w)
               tx = Writer.Transaction.remove(tx, :l)
               {:commit, tx, :ok}
             end)

    assert_eventually do
      assert {:ok,
              [{nil, [{0, :put, :k, :v}, {1, :remove, :k}, {2, :put, :l, :w}, {3, :remove, :l}]}]} ==
               WAL.recover(c.wal)
    end
  end

  test "cannot be in two transactions simultaneously", c do
    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               assert {:error, :already_in_transaction} ==
                        Writer.transaction(c.writer, fn tx ->
                          {:commit, tx, :ok}
                        end)

               {:commit, tx, :ok}
             end)
  end

  test "transactions are not persisted on restarts", c do
    {:registered_name, writer_name} = Process.info(c.writer, :registered_name)
    parent = self()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()

    pid =
      spawn(fn ->
        assert {:error, :no_tx_found} ==
                 Writer.transaction(writer_name, fn tx ->
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
    %{writer: writer} = start_db(c.tmp_dir, @db_opts)

    send(pid, {:ok, ref2})

    receive do
      {:ok, ^ref3} -> :ok
    end

    assert :not_found == Writer.get(writer, :k)
  end

  test "get returns :not_found for non-existent key", c do
    assert :not_found == Writer.get(c.writer, :non_existent)
  end

  test "is_flushing returns false when not flushing", c do
    refute Writer.is_flushing(c.writer)
  end

  @tag db_opts: [task_mod: FakeTask]
  test "is_flushing returns true when flushing", c do
    for n <- 1..10 do
      assert :ok == Writer.put(c.writer, n, "v-#{n}")
    end

    assert Writer.is_flushing(c.writer)
  end

  test "empty transaction commits successfully", c do
    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               {:commit, tx, :ok}
             end)

    assert %{seq: 0} = :sys.get_state(c.writer)
  end

  test "transaction with remove then put on same key", c do
    assert :ok == Writer.put(c.writer, :k, :v1)

    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               tx = Writer.Transaction.remove(tx, :k)
               tx = Writer.Transaction.put(tx, :k, :v2)
               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 2, :v2}} == Writer.get(c.writer, :k)
  end

  test "transaction with put then remove on same key", c do
    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               tx = Writer.Transaction.put(tx, :k, :v)
               tx = Writer.Transaction.remove(tx, :k)
               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 1, nil}} == Writer.get(c.writer, :k)
  end

  test "multiple sequential flushes maintain correct state", c do
    for batch <- 1..3 do
      for n <- 1..10 do
        key = "batch#{batch}_key#{n}"
        assert :ok == Writer.put(c.writer, key, "value#{n}")
      end

      assert_eventually do
        refute Writer.is_flushing(c.writer)
      end
    end

    assert %{seq: 30, flushing: []} = :sys.get_state(c.writer)
  end

  test "transaction can read its own writes", c do
    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               tx = Writer.Transaction.put(tx, :k1, :v1)
               assert {{0, :v1}, tx} = Writer.Transaction.get(tx, :k1)

               tx = Writer.Transaction.put(tx, :k1, :v2)
               assert {{1, :v2}, tx} = Writer.Transaction.get(tx, :k1)

               {:commit, tx, :ok}
             end)

    assert {:ok, {:value, 1, :v2}} == Writer.get(c.writer, :k1)
  end

  test "transaction reads from writer when key not in transaction", c do
    assert :ok == Writer.put(c.writer, :k, :v)

    assert :ok ==
             Writer.transaction(c.writer, fn tx ->
               assert {{0, :v}, tx} = Writer.Transaction.get(tx, :k)
               {:commit, tx, :ok}
             end)
  end
end
