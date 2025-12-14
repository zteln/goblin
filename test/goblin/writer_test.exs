defmodule Goblin.WriterTest do
  @moduledoc """
  Important qualities to test:

  - Non-trivial API:
    - `get/3`
    - `get_multi/2`
    - `iterators/3`
    - `transation/2`
  - Properties:
    - Flushes to DiskTable when exceeding mem limit
    - Can read data during flush
    - Recovers state from WAL on start
    - Transactions are queued
    - Transaction processes are cleaned up on failure
  """
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

  describe "API" do
    test "get/3 returns :not_found for non-existing key" do
      assert :not_found == Writer.get(@table, :k)
    end

    test "get/3 returns value associated with key", c do
      Goblin.put(c.db, :k, :v)
      assert {:value, 0, :v} == Writer.get(@table, :k)
    end

    test "get/3 returns tombstone value if key is removed", c do
      Goblin.put(c.db, :k, :v)
      assert {:value, 0, :v} == Writer.get(@table, :k)
      Goblin.remove(c.db, :k)
      assert {:value, 1, :"$goblin_tombstone"} == Writer.get(@table, :k)
    end

    test "get_multi/2 returns tuple of keys found and keys not found", c do
      Goblin.put(c.db, :k1, :v1)
      Goblin.put(c.db, :k2, :v2)

      assert {[{:k2, 1, :v2}, {:k1, 0, :v1}], []} ==
               Writer.get_multi(@table, [:k1, :k2])

      assert {[], [:k4, :k3]} == Writer.get_multi(@table, [:k3, :k4])

      assert {[{:k1, 0, :v1}], [:k4, :k3]} ==
               Writer.get_multi(@table, [:k1, :k3, :k4])
    end

    test "iterators/3 returns iterators iterating through relevant data", c do
      for n <- 1..10 do
        Goblin.put(c.db, n, "v-#{n}")
      end

      assert {data, next, close} = Writer.iterators(@table, 1, 10)

      data =
        for n <- 1..10, reduce: data do
          acc ->
            key = n
            seq = n - 1
            value = "v-#{n}"
            assert {{^key, ^seq, ^value}, acc} = next.(acc)
            acc
        end

      assert :ok == close.(data)
    end

    test "transaction/2 updates mem table on commit", c do
      assert nil == Goblin.get(c.db, :k)

      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 tx = Goblin.Tx.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end)

      assert :v == Goblin.get(c.db, :k)
    end

    test "transaction/2 does not update mem table when cancelled", c do
      assert nil == Goblin.get(c.db, :k)

      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 _tx = Goblin.Tx.put(tx, :k, :v)
                 :cancel
               end)

      assert nil == Goblin.get(c.db, :k)
    end

    test "transaction/2 does not allow nested transactions", c do
      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 assert {:error, :already_in_tx} ==
                          Writer.transaction(c.writer, fn tx ->
                            {:commit, tx, :ok}
                          end)

                 {:commit, tx, :ok}
               end)
    end

    test "latest_commit_sequence/1 returns default seq (0) if no commits exist" do
      assert 0 == Writer.latest_commit_sequence(@table)
    end

    test "latest_commit_sequence/1 returns latest commit sequence", c do
      Goblin.put(c.db, :k, :v)

      assert 1 == Writer.latest_commit_sequence(@table)

      Goblin.put(c.db, :k, :v)
      Goblin.put(c.db, :k, :v)
      Goblin.put(c.db, :k, :v)

      assert 4 == Writer.latest_commit_sequence(@table)
    end

    @tag db_opts: [mem_limit: 2 * 1024, task_mod: FakeTask]
    test "is_flushing/1 returns boolean indicating whether writer is flushing or not", c do
      refute Writer.is_flushing(c.writer)
      trigger_flush(c.db)
      assert Writer.is_flushing(c.writer)
    end
  end

  describe "property" do
    @tag db_opts: [mem_limit: 2 * 1024]
    test "flushes mem table to disk table(s) when exceeding memory limit", c do
      data = trigger_flush(c.db)

      assert_eventually do
        files = File.ls!(c.tmp_dir)
        assert [sst_file] = Enum.filter(files, &String.match?(&1, ~r/^\d+\.goblin$/))

        assert {:ok, %{key_range: {min, max}}} =
                 Goblin.DiskTable.fetch_sst(Path.join(c.tmp_dir, sst_file))

        assert {^min, _} = Enum.min_by(data, &elem(&1, 0))
        assert {^max, _} = Enum.max_by(data, &elem(&1, 0))
      end
    end

    @tag db_opts: [mem_limit: 2 * 1024, task_mod: FakeTask]
    test "can read during a flush", c do
      data = trigger_flush(c.db)

      assert Goblin.is_flushing(c.db)

      assert {Enum.map(data, fn {k, v} -> {k, k - 1, v} end) |> Enum.reverse(), []} ==
               Writer.get_multi(@table, Enum.map(data, fn {k, _v} -> k end))

      assert Goblin.is_flushing(c.db)
    end

    test "recovers state from WAL on restart", c do
      Goblin.put(c.db, :k, :v)

      stop_db(__MODULE__)
      start_db(c.tmp_dir, name: __MODULE__)

      assert {:value, 0, :v} == Writer.get(@table, :k)
    end

    test "transactions are queued", c do
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

    test "failing transaction holder gets cleaned", c do
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
  end

  defp trigger_flush(db) do
    Stream.iterate(1, &(&1 + 1))
    |> Stream.map(&{&1, "v-#{&1}"})
    |> Stream.chunk_every(1000)
    |> Stream.transform(nil, fn chunk, acc ->
      if Goblin.is_flushing(db) do
        {:halt, acc}
      else
        Goblin.put_multi(db, chunk)
        {chunk, acc}
      end
    end)
    |> Enum.to_list()
  end
end
