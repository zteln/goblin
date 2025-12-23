defmodule Goblin.WriterTest do
  @moduledoc """
  Important qualities to test:

  - Non-trivial API:
    - `get/3`
    - `get_multi/2`
    - `iterators/3`
    - `transation/2`
    - `is_flushing/1`
    - `flush_now/1`
  - Properties:
    - Flushes to DiskTable when exceeding mem limit
    - Can read data during flush
    - Recovers state from WAL on start
    - Correct sequence number on start
    - Transactions are queued
    - Transaction processes are cleaned up on failure
    - Flushes are queued
    - Queued flushes are flushed eventually
    - Flushes are queued on start
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

    @tag db_opts: [task_mod: FakeTask]
    test "is_flushing/1 returns boolean indicating whether writer is flushing or not", c do
      refute Writer.is_flushing(c.writer)
      Writer.flush_now(c.writer)
      assert Writer.is_flushing(c.writer)
    end

    test "flush_now/1 flushes current mem table to disk", c do
      assert {:key, []} == Goblin.Store.get(__MODULE__.Store, :key)
      Goblin.put(c.db, :key, :val)
      assert :ok == Writer.flush_now(c.writer)

      assert_eventually do
        assert {:key, [sst]} = Goblin.Store.get(__MODULE__.Store, :key)
        assert {:ok, {:value, 0, :val}} == Goblin.DiskTable.find(sst, :key)
      end
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

    @tag db_opts: [task_mod: FakeTask]
    test "can read during a flush", c do
      Goblin.put(c.db, :key, :val)
      Writer.flush_now(c.writer)

      assert Goblin.is_flushing(c.db)
      assert {:value, 0, :val} == Writer.get(@table, :key)
      assert Goblin.is_flushing(c.db)
    end

    test "recovers state from WAL on restart", c do
      Goblin.put(c.db, :k, :v)

      stop_db(__MODULE__)
      start_db(c.tmp_dir, name: __MODULE__)

      assert {:value, 0, :v} == Writer.get(@table, :k)
    end

    test "recovers correct sequence number on start", c do
      assert 0 == Writer.latest_commit_sequence(@table)
      Goblin.put(c.db, :k, :v)
      assert 1 == Writer.latest_commit_sequence(@table)

      stop_db(__MODULE__)
      %{db: db} = start_db(c.tmp_dir, name: __MODULE__)

      assert_eventually do
        assert 1 == Writer.latest_commit_sequence(@table)
      end

      Goblin.put(db, :l, :w)
      Goblin.flush_now(db)

      assert_eventually do
        refute Goblin.is_flushing(db)
      end

      stop_db(__MODULE__)
      start_db(c.tmp_dir, name: __MODULE__)

      assert_eventually do
        assert 2 == Writer.latest_commit_sequence(@table)
      end
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

    @tag db_opts: [task_mod: FakeTask]
    test "flushes are queued", c do
      %{flush_queue: flush_queue} = :sys.get_state(c.writer)
      assert :queue.is_empty(flush_queue)

      Goblin.flush_now(c.db)
      %{flushing: flushing, flush_queue: flush_queue} = :sys.get_state(c.writer)
      refute is_nil(flushing)
      assert :queue.is_empty(flush_queue)

      Goblin.flush_now(c.db)
      %{flush_queue: flush_queue} = :sys.get_state(c.writer)
      refute :queue.is_empty(flush_queue)
    end

    @tag db_opts: [task_mod: FakeTask]
    test "queued flushes are flushed eventually", c do
      Goblin.flush_now(c.db)
      Goblin.flush_now(c.db)

      %{flushing: flushing, flush_queue: flush_queue} = :sys.get_state(c.writer)
      assert {ref, _seq, _rotated_wal} = flushing
      refute :queue.is_empty(flush_queue)

      send(c.writer, {ref, {:ok, :flushed}})

      %{flushing: flushing, flush_queue: flush_queue} = :sys.get_state(c.writer)
      assert {ref, _seq, _rotated_wal} = flushing
      assert :queue.is_empty(flush_queue)

      send(c.writer, {ref, {:ok, :flushed}})

      assert %{flushing: nil} = :sys.get_state(c.writer)
    end

    @tag db_opts: [task_mod: FakeTask]
    test "flushes are queued on start", c do
      Goblin.flush_now(c.db)
      Goblin.flush_now(c.db)

      %{flushing: flushing, flush_queue: flush_queue} = :sys.get_state(c.writer)
      assert {_ref, seq, rotated_wal} = flushing
      refute :queue.is_empty(flush_queue)

      stop_db(__MODULE__)
      %{writer: writer} = start_db(c.tmp_dir, name: __MODULE__)

      %{flushing: flushing, flush_queue: flush_queue} = :sys.get_state(writer)
      assert {_ref, ^seq, ^rotated_wal} = flushing
      refute :queue.is_empty(flush_queue)
    end
  end
end
