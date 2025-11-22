defmodule Goblin.ReaderTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog
  alias Goblin.Reader

  @moduletag :tmp_dir

  setup_db(key_limit: 10, level_limit: 512)

  describe "transaction/4" do
    test "reads snapshot of database MemTable", c do
      Process.flag(:trap_exit, true)
      parent = self()
      Goblin.put(c.db, :k1, :v1)
      Goblin.put(c.db, :k2, :v2)

      pid =
        spawn_link(fn ->
          assert :ok ==
                   Reader.transaction(
                     __MODULE__.Writer,
                     __MODULE__.Store,
                     __MODULE__.Reader,
                     c.reader,
                     fn tx ->
                       send(parent, :ready)
                       assert :v1 == Goblin.Tx.get(tx, :k1)
                       assert :v2 == Goblin.Tx.get(tx, :k2)

                       receive do
                         :cont -> :ok
                       end

                       assert :v1 == Goblin.Tx.get(tx, :k1)
                       assert :v2 == Goblin.Tx.get(tx, :k2)

                       send(parent, :done)
                       :ok
                     end
                   )
        end)

      receive do
        :ready -> :ok
      end

      Goblin.put(c.db, :k1, :w1)
      Goblin.put(c.db, :k2, :w2)

      send(pid, :cont)
      assert_receive :done

      assert :w1 == Goblin.get(c.db, :k1)
      assert :w2 == Goblin.get(c.db, :k2)
    end

    test "reads snapshot of database SSTs", c do
      Process.flag(:trap_exit, true)
      parent = self()

      data =
        for n <- 1..10 do
          {n, :"v#{n}"}
        end

      Goblin.put_multi(c.db, data)

      assert_eventually do
        refute Goblin.is_flushing(c.db)
        refute Goblin.is_compacting(c.db)
      end

      pid =
        spawn_link(fn ->
          assert :ok ==
                   Reader.transaction(
                     __MODULE__.Writer,
                     __MODULE__.Store,
                     __MODULE__.Reader,
                     c.reader,
                     fn tx ->
                       send(parent, :ready)

                       for n <- 1..10 do
                         assert :"v#{n}" == Goblin.Tx.get(tx, n)
                       end

                       receive do
                         :cont -> :ok
                       end

                       for n <- 1..10 do
                         assert :"v#{n}" == Goblin.Tx.get(tx, n)
                       end

                       send(parent, :done)

                       :ok
                     end
                   )
        end)

      receive do
        :ready -> :ok
      end

      data =
        for n <- 1..10 do
          {n, :"w#{n}"}
        end

      Goblin.put_multi(c.db, data)

      send(pid, :cont)

      assert_receive :done

      for n <- 1..10 do
        assert :"w#{n}" == Goblin.get(c.db, n)
      end
    end

    test "raises on write", c do
      assert_raise RuntimeError, fn ->
        Reader.transaction(
          __MODULE__.Writer,
          __MODULE__.Store,
          __MODULE__.Reader,
          c.reader,
          fn tx ->
            Goblin.Tx.put(tx, :k, :v)
          end
        )
      end

      assert_raise RuntimeError, fn ->
        Reader.transaction(
          __MODULE__.Writer,
          __MODULE__.Store,
          __MODULE__.Reader,
          c.reader,
          fn tx ->
            Goblin.Tx.remove(tx, :k)
          end
        )
      end
    end

    test "deincs readers if reader raises", c do
      capture_log(fn ->
        Process.flag(:trap_exit, true)
        parent = self()

        reader =
          spawn_link(fn ->
            Reader.transaction(
              __MODULE__.Writer,
              __MODULE__.Store,
              __MODULE__.Reader,
              c.reader,
              fn _tx ->
                send(parent, :ready)

                receive do
                  :cont -> :ok
                end

                raise "reader failure"
              end
            )
          end)

        assert_receive :ready

        spawn_link(fn ->
          send(parent, :ready)
          assert :ok == Reader.empty?(c.reader)
          send(parent, :done)
        end)

        assert_receive :ready
        refute_receive :done

        send(reader, :cont)

        assert_receive :done

        assert_receive {:EXIT, ^reader, _reason}
      end)
    end
  end

  describe "get/4" do
    test "returns :not_found for non-existing key" do
      assert :not_found == Reader.get(:k, __MODULE__.Writer, __MODULE__.Store)
    end

    test "returns value corresponding to key", c do
      Goblin.put(c.db, :k, :v)

      assert {0, :v} == Reader.get(:k, __MODULE__.Writer, __MODULE__.Store)

      for n <- 1..10 do
        Goblin.put(c.db, :"k#{n}", :"v#{n}")
      end

      for n <- 1..10 do
        assert {n, :"v#{n}"} == Reader.get(:"k#{n}", __MODULE__.Writer, __MODULE__.Store)
      end
    end
  end

  describe "get_multi/3" do
    test "returns existing key-value pairs from provided keys", c do
      max = 15

      for n <- 1..max//2 do
        Goblin.put(c.db, :"k#{n}", :"v#{n}")
      end

      assert for(n <- 1..max//2, do: {:"k#{n}", div(n, 2), :"v#{n}"}) |> List.keysort(0) ==
               Reader.get_multi(
                 for(n <- 1..max, do: :"k#{n}"),
                 __MODULE__.Writer,
                 __MODULE__.Store
               )
               |> List.keysort(0)
    end
  end

  describe "select/4" do
    test "returns stream over all key-value pairs in sorted order", c do
      data =
        for n <- 1..15 do
          {:"k#{n}", :"v#{n}"}
        end

      output =
        for n <- 1..15 do
          {:"k#{n}", n - 1, :"v#{n}"}
        end

      Goblin.put_multi(c.db, data)

      assert List.keysort(output, 0) ==
               Reader.select(nil, nil, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()
    end

    test "returns stream over subset of key-value pairs", c do
      data =
        for n <- 1..15 do
          {n, :"v#{n}"}
        end

      output =
        for n <- 1..15 do
          {n, n - 1, :"v#{n}"}
        end

      Goblin.put_multi(c.db, data)

      assert List.keysort(Enum.take(output, 7), 0) ==
               Reader.select(nil, 7, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()

      assert List.keysort(Enum.take(output, -7), 0) ==
               Reader.select(9, nil, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()

      assert List.keysort(Enum.slice(output, 2, 9), 0) ==
               Reader.select(3, 11, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()
    end
  end
end
