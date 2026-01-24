defmodule Goblin.BrokerTest do
  use ExUnit.Case, async: true
  use TestHelper
  use Mimic
  import ExUnit.CaptureLog

  setup_db(
    mem_limit: 2 * 1024,
    bf_bit_array_size: 1000
  )

  @mem_table __MODULE__.MemTable
  @disk_tables __MODULE__.DiskTables
  @cleaner_table __MODULE__.Cleaner

  test "can have multiple readers simultanously", c do
    parent = self()

    reader1 =
      spawn(fn ->
        Goblin.Broker.read_transaction(
          @cleaner_table,
          c.cleaner,
          @mem_table,
          @disk_tables,
          fn _tx ->
            send(parent, :ready)

            receive do
              :done -> :ok
            end
          end
        )
      end)

    assert_receive :ready

    reader2 =
      spawn(fn ->
        Goblin.Broker.read_transaction(
          @cleaner_table,
          c.cleaner,
          @mem_table,
          @disk_tables,
          fn _tx ->
            send(parent, :ready)

            receive do
              :done -> :ok
            end
          end
        )
      end)

    assert_receive :ready

    send(reader1, :done)
    send(reader2, :done)
  end

  test "readers can only read from their snapshot", c do
    parent = self()
    [key] = StreamData.term() |> Enum.take(1)
    [val1, val2] = StreamData.term() |> Enum.take(2)

    Goblin.put(c.db, key, val1)

    reader =
      spawn(fn ->
        Goblin.Broker.read_transaction(
          @cleaner_table,
          c.cleaner,
          @mem_table,
          @disk_tables,
          fn tx ->
            send(parent, :ready)

            receive do
              :read_key -> :ok
            end

            assert val1 == Goblin.Tx.get(tx, key)
            send(parent, :done)
          end
        )
      end)

    assert_receive :ready
    Goblin.put(c.db, key, val2)
    send(reader, :read_key)
    assert_receive :done
  end

  test "writers are queued", c do
    parent = self()

    writer1 =
      spawn(fn ->
        assert :ok ==
                 Goblin.Broker.write_transaction(c.broker, fn tx ->
                   send(parent, :ready)

                   receive do
                     :done -> :ok
                   end

                   {:commit, tx, :ok}
                 end)
      end)

    assert_receive :ready

    writer2 =
      spawn(fn ->
        assert :ok ==
                 Goblin.Broker.write_transaction(c.broker, fn tx ->
                   send(parent, :ready)

                   receive do
                     :done -> :ok
                   end

                   {:commit, tx, :ok}
                 end)

        send(parent, :done)
      end)

    refute_receive :ready
    send(writer1, :done)
    assert_receive :ready
    send(writer2, :done)
    assert_receive :done
  end

  test "writes are inserted into memtable", c do
    [key] = StreamData.term() |> Enum.take(1)
    [val] = StreamData.term() |> Enum.take(1)

    assert :ok ==
             Goblin.Broker.write_transaction(c.broker, fn tx ->
               tx = Goblin.Tx.put(tx, key, val)
               {:commit, tx, :ok}
             end)

    assert [{:value, {key, 0, val}}] == Goblin.MemTable.get_multi(@mem_table, [key], 1)
  end

  test "failed writers are cleaned up", c do
    parent = self()

    writer1 =
      spawn(fn ->
        assert :ok ==
                 Goblin.Broker.write_transaction(c.broker, fn _tx ->
                   send(parent, :ready)

                   receive do
                     :done -> :ok
                   end

                   Process.exit(self(), :normal)
                 end)
      end)

    assert_receive :ready

    spawn(fn ->
      assert :ok ==
               Goblin.Broker.write_transaction(c.broker, fn tx ->
                 {:commit, tx, :ok}
               end)

      send(parent, :done)
    end)

    send(writer1, :done)
    assert_receive :done
  end

  test "aborted transactions are not committed", c do
    [key] = StreamData.term() |> Enum.take(1)
    [val] = StreamData.term() |> Enum.take(1)

    assert {:error, :aborted} ==
             Goblin.Broker.write_transaction(c.broker, fn tx ->
               Goblin.Tx.put(tx, key, val)
               :abort
             end)

    refute val == Goblin.get(c.db, key)
  end

  test "commits are broadcasted", c do
    [key1, key2] = StreamData.term() |> Enum.take(2)
    [val] = StreamData.term() |> Enum.take(1)

    Goblin.subscribe(c.db)

    assert :ok ==
             Goblin.Broker.write_transaction(c.broker, fn tx ->
               tx =
                 tx
                 |> Goblin.Tx.put(key1, val)
                 |> Goblin.Tx.remove(key2)

               {:commit, tx, :ok}
             end)

    assert_receive {:put, ^key1, ^val}
    assert_receive {:remove, ^key2}
  end

  test "broker server stops if MemTable is unable to insert writes", %{broker: broker} do
    Process.monitor(broker)
    Process.flag(:trap_exit, true)

    [key] = StreamData.term() |> Enum.take(1)
    [val] = StreamData.term() |> Enum.take(1)

    Goblin.MemTable
    |> expect(:insert, fn _mem_table_server, _writes, _seq ->
      {:error, :unable_to_insert_writes}
    end)

    Goblin.MemTable
    |> allow(self(), broker)

    {_result, _log} =
      with_log(fn ->
        Goblin.Broker.write_transaction(broker, fn tx ->
          tx = Goblin.Tx.put(tx, key, val)
          {:commit, tx, :ok}
        end)

        assert_receive {:DOWN, _ref, :process, ^broker, :unable_to_insert_writes}
      end)
  end
end
