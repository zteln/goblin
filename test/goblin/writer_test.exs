defmodule Goblin.WriterTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog
  alias Goblin.Writer

  defmodule FailingTask do
    def async(_sup, _f) do
      Task.async(fn -> {:error, :failed} end)
    end
  end

  @moduletag :tmp_dir

  describe "get/3, get_multi/3" do
    setup c, do: start_writer(c.tmp_dir)

    test "returns :not_found for non-existing key" do
      assert :not_found == Writer.get(__MODULE__, :k)
    end

    test "returns corresponding value", c do
      Writer.put(c.writer, :k, :v)
      assert {:value, 0, :v} == Writer.get(__MODULE__, :k)
    end

    test "returns tuple of keys found and keys not found", c do
      Writer.put(c.writer, :k1, :v1)
      Writer.put(c.writer, :k2, :v2)

      assert {[{:k2, 1, :v2}, {:k1, 0, :v1}], []} ==
               Writer.get_multi(__MODULE__, [:k1, :k2])

      assert {[], [:k4, :k3]} == Writer.get_multi(__MODULE__, [:k3, :k4])
      assert {[{:k1, 0, :v1}], [:k4, :k3]} == Writer.get_multi(__MODULE__, [:k1, :k3, :k4])
    end

    test "returns :tombstone as value if key is removed", c do
      Writer.remove(c.writer, :k)
      assert {:value, 0, :tombstone} == Writer.get(__MODULE__, :k)
    end
  end

  describe "iterators/3" do
    setup c, do: start_writer(c.tmp_dir)

    test "returns empty range if MemTable is empty" do
      assert {[], _} = Writer.iterators(__MODULE__, nil, nil)
    end

    test "returns iterator over MemTable data", c do
      Writer.put(c.writer, :k1, :v1)
      Writer.put(c.writer, :k2, :v2)

      assert {[{:k1, 0, :v1}, {:k2, 1, :v2}], _} =
               Writer.iterators(__MODULE__, nil, nil)
    end

    test "returns iterator over MemTable subset of data", c do
      Writer.put(c.writer, :k1, :v1)
      Writer.put(c.writer, :k2, :v2)
      Writer.put(c.writer, :k3, :v3)

      assert {[{:k2, 1, :v2}], _} = Writer.iterators(__MODULE__, :k2, :k2)
      assert {[{:k1, 0, :v1}, {:k2, 1, :v2}], _} = Writer.iterators(__MODULE__, nil, :k2)
      assert {[{:k2, 1, :v2}, {:k3, 2, :v3}], _} = Writer.iterators(__MODULE__, :k2, nil)
    end

    test "iterates over provided range", c do
      Writer.put(c.writer, :k1, :v1)
      Writer.put(c.writer, :k2, :v2)
      Writer.put(c.writer, :k3, :v3)

      assert {[{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}] = range, iter_f} =
               Writer.iterators(__MODULE__, nil, nil)

      assert {{:k1, 0, :v1}, range} = iter_f.(range)
      assert {{:k2, 1, :v2}, range} = iter_f.(range)
      assert {{:k3, 2, :v3}, range} = iter_f.(range)
      assert :ok == iter_f.(range)
    end
  end

  describe "put/3, put_multi/2, remove/2, remove_multi/2" do
    setup c, do: start_writer(c.tmp_dir, Map.get(c, :extra_opts, []))

    test "works as expected", c do
      assert :not_found == Writer.get(__MODULE__, :k)
      assert :ok == Writer.put(c.writer, :k, :v)
      assert {:value, 0, :v} == Writer.get(__MODULE__, :k)
      assert :ok == Writer.remove(c.writer, :k)
      assert {:value, 1, :tombstone} == Writer.get(__MODULE__, :k)

      assert :ok == Writer.put_multi(c.writer, [{:k1, :v1}, {:k2, :v2}])
      assert {[{:k2, 3, :v2}, {:k1, 2, :v1}], []} == Writer.get_multi(__MODULE__, [:k1, :k2])
      assert :ok == Writer.remove_multi(c.writer, [:k1, :k2])

      assert {[{:k2, 5, :tombstone}, {:k1, 4, :tombstone}], []} ==
               Writer.get_multi(__MODULE__, [:k1, :k2])
    end

    test "flushes to SST if MemTable exceeds key limit", c do
      data =
        for n <- 1..10 do
          {:"k#{n}", :"v#{n}"}
        end

      files = File.ls!(c.tmp_dir)
      no_of_files = length(files)

      assert :ok == Writer.put_multi(c.writer, data)

      assert_eventually do
        new_files = File.ls!(c.tmp_dir)
        assert no_of_files + 1 == length(new_files)
        [sst_file] = new_files -- files

        assert {:ok, %{seq_range: {0, 9}, key_range: {:k1, :k9}}} =
                 Goblin.SSTs.fetch_sst(Path.join(c.tmp_dir, sst_file))
      end

      for n <- 1..10 do
        assert :not_found == Writer.get(__MODULE__, :"k#{n}")
      end
    end

    @tag extra_opts: [task_mod: FailingTask]
    test "retries flush on failure", c do
      writer = c.writer
      Process.flag(:trap_exit, true)

      data =
        for n <- 1..10 do
          {:"k#{n}", :"v#{n}"}
        end

      log =
        capture_log(fn ->
          assert :ok == Writer.put_multi(c.writer, data)
          assert_receive {:EXIT, ^writer, {:error, :failed_to_flush}}
        end)

      assert log =~ "Failed to flush with reason: :failed. Retrying..."
      assert log =~ "Failed to flush after 5 attempts with reason: :failed. Exiting."
    end
  end

  describe "transaction/2" do
    setup c, do: start_writer(c.tmp_dir)

    test "commits writes to MemTable", c do
      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 tx = Writer.Transaction.put(tx, :k, :v)
                 {:commit, tx, :ok}
               end)

      assert {:value, 0, :v} == Writer.get(__MODULE__, :k)
    end

    test "if cancelled does not commit to MemTable", c do
      assert :ok ==
               Writer.transaction(c.writer, fn tx ->
                 Writer.Transaction.put(tx, :k, :v)
                 :cancel
               end)

      assert :not_found == Writer.get(__MODULE__, :k)
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

      spawn(fn ->
        assert :ok ==
                 Writer.transaction(c.writer, fn tx ->
                   {:commit, tx, :ok}
                 end)
      end)

      receive do
        :ready -> :ok
      end

      assert %{write_queue: wq} = :sys.get_state(c.writer)
      refute :queue.is_empty(wq)

      send(pid1, :cont)

      assert_eventually do
        assert %{write_queue: wq} = :sys.get_state(c.writer)
        assert :queue.is_empty(wq)
      end
    end
  end

  defp start_writer(dir, opts \\ []) do
    wal =
      start_link_supervised!({Goblin.WAL, name: __MODULE__.WAL, db_dir: dir},
        id: __MODULE__.WAL
      )

    manifest =
      start_link_supervised!({Goblin.Manifest, name: __MODULE__.Manifest, db_dir: dir},
        id: __MODULE__.Manifest
      )

    compactor =
      start_link_supervised!(
        {Goblin.Compactor,
         name: __MODULE__.Compactor,
         store: __MODULE__.Store,
         manifest: __MODULE__.Manifest,
         task_sup: __MODULE__.TaskSupervisor,
         level_limit: 512,
         key_limit: 10},
        id: __MODULE__.Compactor
      )

    store =
      start_link_supervised!(
        {Goblin.Store,
         name: __MODULE__.Store,
         db_dir: dir,
         manifest: __MODULE__.Manifest,
         compactor: __MODULE__.Compactor},
        id: __MODULE__.Store
      )

    start_link_supervised!(
      {Goblin.PubSub, name: __MODULE__.PubSub},
      id: __MODULE__.PubSub
    )

    start_link_supervised!({Task.Supervisor, name: __MODULE__.TaskSupervisor},
      id: __MODULE__.TaskSupervisor
    )

    opts =
      [
        name: __MODULE__,
        local_name: __MODULE__,
        db_dir: dir,
        store: __MODULE__.Store,
        manifest: __MODULE__.Manifest,
        wal: __MODULE__.WAL,
        task_sup: __MODULE__.TaskSupervisor,
        pub_sub: __MODULE__.PubSub,
        key_limit: 10
      ]
      |> Keyword.merge(opts)

    writer =
      start_link_supervised!({Goblin.Writer, opts}, id: __MODULE__)

    %{writer: writer, manifest: manifest, wal: wal, compactor: compactor, store: store}
  end
end
