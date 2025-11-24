defmodule Goblin.WriterTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Writer

  defmodule FakeTask do
    def async(_sup, _f) do
      %{ref: make_ref()}
    end
  end

  @table __MODULE__.Writer
  @moduletag :tmp_dir
  setup_db()

  describe "get/3, get_multi/3" do
    test "returns :not_found for non-existing key" do
      assert :not_found == Writer.get(@table, :k)
    end

    test "returns corresponding value", c do
      put(c.writer, :k, :v)
      assert {:value, 0, :v} == Writer.get(@table, :k)
    end

    test "returns tuple of keys found and keys not found", c do
      put(c.writer, :k1, :v1)
      put(c.writer, :k2, :v2)

      assert {[{:k2, 1, :v2}, {:k1, 0, :v1}], []} ==
               Writer.get_multi(@table, [:k1, :k2])

      assert {[], [:k4, :k3]} == Writer.get_multi(@table, [:k3, :k4])
      assert {[{:k1, 0, :v1}], [:k4, :k3]} == Writer.get_multi(@table, [:k1, :k3, :k4])
    end

    test "returns :$goblin_tombstone as value if key is removed", c do
      remove(c.writer, :k)
      assert {:value, 0, :"$goblin_tombstone"} == Writer.get(@table, :k)
    end

    @tag db_opts: [task_mod: FakeTask, key_limit: 10]
    test "can read during flush", c do
      data =
        for n <- 1..10 do
          {:"k#{n}", :"v#{n}"}
        end

      put_multi(c.writer, data)

      assert %{flushing: {_, _, _}} = :sys.get_state(c.writer)

      for n <- 1..10 do
        assert {:value, n - 1, :"v#{n}"} == Writer.get(@table, :"k#{n}")
      end

      keys = for(n <- 1..10, do: :"k#{n}")

      assert {for(n <- 10..1//-1, do: {:"k#{n}", n - 1, :"v#{n}"}), []} ==
               Writer.get_multi(@table, keys)
    end
  end

  describe "iterators/3" do
    test "returns empty range if MemTable is empty" do
      assert {[], _, _} = Writer.iterators(@table, nil, nil)
    end

    test "returns iterator over MemTable data", c do
      put(c.writer, :k1, :v1)
      put(c.writer, :k2, :v2)

      assert {[{:k1, 0, :v1}, {:k2, 1, :v2}], _, _} =
               Writer.iterators(@table, nil, nil)
    end

    test "returns iterator over MemTable subset of data", c do
      put(c.writer, :k1, :v1)
      put(c.writer, :k2, :v2)
      put(c.writer, :k3, :v3)

      assert {[{:k2, 1, :v2}], _, _} = Writer.iterators(@table, :k2, :k2)
      assert {[{:k1, 0, :v1}, {:k2, 1, :v2}], _, _} = Writer.iterators(@table, nil, :k2)
      assert {[{:k2, 1, :v2}, {:k3, 2, :v3}], _, _} = Writer.iterators(@table, :k2, nil)
    end

    test "iterates over provided range", c do
      put(c.writer, :k1, :v1)
      put(c.writer, :k2, :v2)
      put(c.writer, :k3, :v3)

      assert {[{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}] = state, next, close} =
               Writer.iterators(@table, nil, nil)

      assert {{:k1, 0, :v1}, state} = next.(state)
      assert {{:k2, 1, :v2}, state} = next.(state)
      assert {{:k3, 2, :v3}, state} = next.(state)
      assert :ok == next.(state)
      assert :ok == close.(state)
    end
  end

  describe "transaction/2" do
    test "commits writes to MemTable", c do
      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 tx = Goblin.Tx.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end)

      assert {:value, 0, :v} == Writer.get(@table, :k)
    end

    test "if cancelled does not commit to MemTable", c do
      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 Goblin.Tx.put(tx, :k, :v)
                 :cancel
               end)

      assert :not_found == Writer.get(@table, :k)
    end

    test "writer state cleans writer if writer pid exits mid-transaction", c do
      parent = self()

      pid =
        spawn(fn ->
          Writer.transaction(c.writer, fn _tx ->
            send(parent, :ready)

            receive do
              :cont -> :ok
            end

            exit(:normal)
          end)
        end)

      receive do
        :ready -> :ok
      end

      assert %{writer: {^pid, _ref}} = :sys.get_state(c.writer)
      send(pid, :cont)

      assert_eventually do
        assert %{writer: nil} = :sys.get_state(c.writer)
      end
    end

    test "writers are queued", c do
      parent = self()

      pid1 =
        spawn(fn ->
          assert :ok ==
                   Writer.transaction(c.writer, fn tx ->
                     send(parent, :ready)

                     receive do
                       :cont -> :ok
                     end

                     {:commit, tx, :ok}
                   end)
        end)

      receive do
        :ready -> :ok
      end

      spawn(fn ->
        assert :ok ==
                 Writer.transaction(c.writer, fn tx ->
                   {:commit, tx, :ok}
                 end)
      end)

      assert_eventually do
        assert %{write_queue: wq} = :sys.get_state(c.writer)
        refute :queue.is_empty(wq)
      end

      send(pid1, :cont)

      assert_eventually do
        assert %{write_queue: wq} = :sys.get_state(c.writer)
        assert :queue.is_empty(wq)
      end
    end

    @tag db_opts: [key_limit: 10]
    test "flushes to SST if MemTable exceeds key limit", c do
      data =
        for n <- 1..10 do
          {:"k#{n}", :"v#{n}"}
        end

      assert :ok == put_multi(c.writer, data)

      assert_eventually do
        files = File.ls!(c.tmp_dir)
        assert [sst_file] = Enum.filter(files, &String.match?(&1, ~r/^\d+\.goblin$/))

        assert {:ok, %{seq_range: {0, 9}, key_range: {:k1, :k9}}} =
                 Goblin.SSTs.fetch_sst(Path.join(c.tmp_dir, sst_file))
      end

      for n <- 1..10 do
        assert :not_found == Writer.get(@table, :"k#{n}")
      end
    end
  end

  defp remove(writer, k) do
    Writer.transaction(writer, fn tx ->
      tx = Goblin.Tx.remove(tx, k)
      {:commit, tx, :ok}
    end)
  end

  defp put(writer, k, v) do
    Writer.transaction(writer, fn tx ->
      tx = Goblin.Tx.put(tx, k, v)
      {:commit, tx, :ok}
    end)
  end

  defp put_multi(writer, pairs) do
    Goblin.Writer.transaction(writer, fn tx ->
      tx =
        Enum.reduce(pairs, tx, fn {k, v}, acc ->
          Goblin.Tx.put(acc, k, v)
        end)

      {:commit, tx, :ok}
    end)
  end
end
