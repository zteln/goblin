defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog
  alias Goblin.Compactor

  @moduletag :tmp_dir

  defmodule FailingTask do
    def async(_sup, _), do: Task.async(fn -> {:error, :failed} end)
  end

  describe "put/3" do
    setup c, do: start_compactor(c.tmp_dir, Map.get(c, :extra_opts, []))

    test "updates state", c do
      assert %{levels: %{} = levels} = :sys.get_state(c.compactor)
      assert map_size(levels) == 0

      assert :ok == Compactor.put(c.compactor, 0, "foo", 0, 1, {1, 2})

      assert %{
               levels: %{
                 0 => %{
                   entries: %{
                     "foo" => %{
                       id: "foo",
                       priority: 0,
                       size: 1,
                       key_range: {1, 2}
                     }
                   }
                 }
               }
             } = :sys.get_state(c.compactor)
    end

    test "exceeding level limit causes a compaction", c do
      file = Path.join(c.tmp_dir, "foo")
      fake_sst(file, [{1, 0, "v-1"}])
      %{size: size} = File.stat!(file)

      assert :ok == Compactor.put(c.compactor, 0, file, 0, size, {1, 1})

      assert_eventually do
        assert %{levels: %{1 => %{entries: entries}}} = :sys.get_state(c.compactor)
        assert [%{priority: 0, size: ^size, key_range: {1, 1}}] = Map.values(entries)
      end
    end

    test "two ssts are eventually merged after compaction", c do
      file1 = Path.join(c.tmp_dir, "foo")
      fake_sst(file1, [{1, 0, "v-1"}])
      %{size: size1} = File.stat!(file1)
      file2 = Path.join(c.tmp_dir, "bar")
      fake_sst(file2, [{2, 1, "v-2"}])
      %{size: size2} = File.stat!(file2)

      assert :ok == Compactor.put(c.compactor, 0, file1, 0, size1, {1, 1})
      assert :ok == Compactor.put(c.compactor, 0, file2, 1, size2, {2, 2})

      assert_eventually do
        assert %{levels: %{1 => %{entries: entries}}} = :sys.get_state(c.compactor)
        assert [%{priority: 0, size: size, key_range: {1, 2}}] = Map.values(entries)
        assert size < size1 + size2
      end
    end

    @tag extra_opts: [task_mod: FailingTask]
    test "compaction is retried 5 times", c do
      Process.flag(:trap_exit, true)
      file1 = Path.join(c.tmp_dir, "foo")
      fake_sst(file1, [{1, 0, "v-1"}])
      %{size: size1} = File.stat!(file1)

      log =
        capture_log(fn ->
          assert :ok == Compactor.put(c.compactor, 0, file1, 0, size1, {1, 1})
          assert_receive {:EXIT, _pid, {:error, :failed_to_compact}}
        end)

      assert log =~ "Failed to compact with reason: :failed. Retrying..."
      assert log =~ "Failed to compact after 5 attempts with reason: :failed. Exiting."
    end

    test "files are only cleaned if there are no active readers", c do
      fake_write_table = :ets.new(:fake_write_table, [:named_table])

      reader =
        spawn_link(fn ->
          Goblin.Reader.transaction(fake_write_table, nil, __MODULE__.Reader, __MODULE__.Reader, fn _tx ->
            receive do
              :cont -> :ok
            end
          end)
        end)

      file = Path.join(c.tmp_dir, "foo")
      fake_sst(file, [{1, 0, "v-1"}])
      %{size: size} = File.stat!(file)

      assert :ok == Compactor.put(c.compactor, 0, file, 0, size, {1, 1})

      assert_eventually do
        assert %{clean_ups: [{[^file], _, _}]} = :sys.get_state(c.compactor)
        assert File.exists?(file)
      end

      send(reader, :cont)

      assert_eventually do
        assert %{clean_ups: []} = :sys.get_state(c.compactor)
        refute File.exists?(file)
      end
    end
  end

  defp start_compactor(dir, opts) do
    manifest =
      start_link_supervised!({Goblin.Manifest, name: __MODULE__.Manifest, db_dir: dir},
        id: __MODULE__.Manifest
      )

    store =
      start_link_supervised!(
        {Goblin.Store,
         name: __MODULE__.Store, db_dir: dir, manifest: __MODULE__.Manifest, compactor: __MODULE__},
        id: __MODULE__.Store
      )

    start_link_supervised!(
      {Goblin.Reader, name: __MODULE__.Reader},
      id: __MODULE__.Reader
    )

    start_link_supervised!({Task.Supervisor, name: __MODULE__.TaskSupervisor},
      id: __MODULE__.TaskSupervisor
    )

    opts =
      [
        name: __MODULE__,
        store: __MODULE__.Store,
        manifest: __MODULE__.Manifest,
        reader: __MODULE__.Reader,
        task_sup: __MODULE__.TaskSupervisor,
        key_limit: 10,
        level_limit: 512
      ]
      |> Keyword.merge(opts)

    compactor =
      start_link_supervised!(
        {Goblin.Compactor, opts},
        id: __MODULE__
      )

    %{compactor: compactor, manifest: manifest, store: store}
  end
end
