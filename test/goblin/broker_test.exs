defmodule Goblin.BrokerTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper
  use Mimic

  import ExUnit.CaptureLog

  alias Goblin.MemTables

  setup_db()

  describe "contract" do
    test "write transactions commit data", c do
      assert :ok ==
               Goblin.transaction(c.db, fn tx ->
                 tx = Goblin.Tx.put(tx, :key, :val)
                 {:commit, tx, :ok}
               end)

      assert :val == Goblin.get(c.db, :key)
    end

    test "write transactions are serialized", c do
      parent = self()

      # writer 1 holds the lock
      writer1 =
        spawn(fn ->
          Goblin.transaction(c.db, fn tx ->
            send(parent, :writer1_started)

            receive do
              :continue -> :ok
            end

            {:commit, Goblin.Tx.put(tx, :k1, :v1), :ok}
          end)

          send(parent, :writer1_done)
        end)

      assert_receive :writer1_started

      # writer 2 is queued — cannot start until writer 1 commits
      spawn(fn ->
        Goblin.transaction(c.db, fn tx ->
          send(parent, :writer2_started)
          {:commit, Goblin.Tx.put(tx, :k2, :v2), :ok}
        end)

        send(parent, :writer2_done)
      end)

      # writer 2 should not have started yet
      refute_receive :writer2_started, 100

      # release writer 1
      send(writer1, :continue)
      assert_receive :writer1_done

      # writer 2 should now proceed
      assert_receive :writer2_started
      assert_receive :writer2_done

      assert :v1 == Goblin.get(c.db, :k1)
      assert :v2 == Goblin.get(c.db, :k2)
    end

    test "aborted transactions are not committed", c do
      assert {:error, :aborted} ==
               Goblin.transaction(c.db, fn tx ->
                 Goblin.Tx.put(tx, :key, :val)
                 :abort
               end)

      assert nil == Goblin.get(c.db, :key)
    end
  end

  describe "failure handling" do
    test "crashed writer releases the lock", c do
      parent = self()

      # writer 1 will crash
      pid =
        spawn(fn ->
          Goblin.transaction(c.db, fn tx ->
            send(parent, :writer1_started)

            receive do
              :crash -> exit(:crash)
            end

            {:commit, tx, :ok}
          end)
        end)

      assert_receive :writer1_started

      # writer 2 is queued
      spawn(fn ->
        result =
          Goblin.transaction(c.db, fn tx ->
            {:commit, Goblin.Tx.put(tx, :key, :val), :ok}
          end)

        send(parent, {:writer2_done, result})
      end)

      # writer 2 should be blocked
      refute_receive {:writer2_done, _}, 100

      # crash writer 1
      Process.exit(pid, :kill)

      # writer 2 should proceed
      assert_receive {:writer2_done, :ok}, 1000
      assert :val == Goblin.get(c.db, :key)
    end

    test "commits are broadcast to subscribers", c do
      Goblin.subscribe(c.db)

      Goblin.transaction(c.db, fn tx ->
        tx =
          tx
          |> Goblin.Tx.put(:key, :val)
          |> Goblin.Tx.remove(:other_key)

        {:commit, tx, :ok}
      end)

      assert_receive {:put, :key, :val}
      assert_receive {:remove, :other_key}
    end

    test "stops when MemTables fails to write", %{broker: broker} = c do
      Process.monitor(broker)
      Process.flag(:trap_exit, true)

      MemTables
      |> expect(:write, fn _server, _seq, _writes ->
        {:error, :write_failed}
      end)

      MemTables
      |> allow(self(), broker)

      with_log(fn ->
        try do
          Goblin.transaction(c.db, fn tx ->
            {:commit, Goblin.Tx.put(tx, :key, :val), :ok}
          end)
        rescue
          ArgumentError -> :ok
        catch
          :exit, _ -> :ok
        end

        assert_receive {:DOWN, _ref, :process, ^broker, :write_failed}
      end)
    end
  end
end
