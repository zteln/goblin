defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog
  alias Goblin.Compactor
  alias Goblin.Compactor.Level
  alias Goblin.Compactor.Entry

  defmodule FakeTask do
    def async(_sup, _f) do
      %{ref: :fake_ref}
    end
  end

  defmodule FailingTask do
    def async(_sup, _f) do
      Task.async(fn -> {:error, :failed_to_compact} end)
    end
  end

  @moduletag :tmp_dir
  @db_opts [
    id: :compactor_test,
    name: :compactor_test,
    key_limit: 10,
    sync_interval: 50,
    level_limit: 100,
    wal_name: :compactor_test_wal,
    manifest_name: :compactor_test_manifest
  ]

  setup_db(@db_opts)

  test "compactor starts with empty levels", c do
    assert %{levels: levels} = :sys.get_state(c.compactor)
    assert %{} == levels
  end

  test "put/4 puts file in level queue for compaction", c do
    assert :ok == Compactor.put(c.registry, 0, {"foo", 0, 1, {2, 3}})

    assert %{
             levels: %{
               0 => %Level{
                 entries: %{"foo" => %Entry{id: "foo", priority: 0, size: 1, key_range: {2, 3}}}
               }
             }
           } = :sys.get_state(c.compactor)
  end

  test "level with size exceeding level_limit causes a merge", c do
    assert :ok == Compactor.put(c.registry, 0, {"foo", 0, 50, {2, 3}})
    assert %{levels: %{0 => %Level{compacting_ref: nil}}} = :sys.get_state(c.compactor)

    assert :ok == Compactor.put(c.registry, 0, {"bar", 1, 50, {5, 7}})

    assert %{
             levels: %{
               0 => %Level{compacting_ref: {ref, _}},
               1 => %Level{compacting_ref: {ref, _}}
             }
           } = :sys.get_state(c.compactor)

    assert is_reference(ref)
  end

  test "a completed compaction cleans up sources", c do
    file1 = write_sst(c.tmp_dir, "foo", 0, 10, [{0, 1, :v1}, {1, 2, :v2}])
    file2 = write_sst(c.tmp_dir, "bar", 0, 10, [{2, 3, :v3}, {3, 4, :v4}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)
    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 2}})
    assert :ok == Compactor.put(c.registry, 0, {file2, 2, size2, {3, 4}})
    assert File.exists?(file1)
    assert File.exists?(file2)

    assert_eventually do
      assert %{
               levels: %{
                 2 =>
                   %Level{
                     compacting_ref: nil,
                     entries: entries
                   } = levels
               }
             } = :sys.get_state(c.compactor)

      assert nil == Map.get(levels, 0)
      assert nil == Map.get(levels, 1)

      assert [%{id: file, priority: 0, size: size, key_range: {1, 4}}] = Map.values(entries)
      assert size < size1 + size2

      assert [
               {0, 1, :v1},
               {1, 2, :v2},
               {2, 3, :v3},
               {3, 4, :v4}
             ] == file |> Goblin.SSTs.stream!() |> Enum.to_list()
    end

    refute File.exists?(file1)
    refute File.exists?(file2)
  end

  @tag db_opts: [task_mod: FailingTask]
  test "failed compactions are retried until Compactor exits", c do
    Process.monitor(c.compactor)
    Process.flag(:trap_exit, true)
    file = write_sst(c.tmp_dir, "foo", 1, 10, [{0, 1, :v1}, {1, 2, :v2}])
    %{size: size} = File.stat!(file)

    log =
      capture_log(fn ->
        assert :ok == Compactor.put(c.registry, 1, {file, 0, size, {1, 2}})

        assert_receive {:DOWN, _ref, :process, _pid, {:error, :failed_to_compact}}
      end)

    assert log =~ "Failed to compact with reason: :failed_to_compact. Retrying..."
    assert log =~ "Failed to compact after 5 attempts with reason: :failed_to_compact. Exiting."
  end

  @tag db_opts: [task_mod: FakeTask]
  test "compactions on same levels are sequential", c do
    file1 = write_sst(c.tmp_dir, "foo", 1, 10, [{0, 1, :v1}, {1, 2, :v2}])
    file2 = write_sst(c.tmp_dir, "bar", 1, 10, [{2, 3, :v3}, {3, 4, :v4}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)
    assert :ok == Compactor.put(c.registry, 1, {file1, 0, size1, {1, 2}})
    assert :ok == Compactor.put(c.registry, 1, {file2, 2, size2, {3, 4}})

    assert %{
             levels: %{
               1 => %{
                 compacting_ref: {:fake_ref, _},
                 entries: entries
               },
               2 => %{
                 compacting_ref: {:fake_ref, _}
               }
             }
           } = :sys.get_state(c.compactor)

    assert %{id: ^file1, priority: 0} = Map.get(entries, file1)
    assert %{id: ^file2, priority: 2} = Map.get(entries, file2)

    send(c.compactor, {:fake_ref, {:ok, [file1], 1, 2}})

    assert %{
             levels: %{
               1 => %{
                 compacting_ref: {:fake_ref, _},
                 entries: entries
               },
               2 => %{
                 compacting_ref: {:fake_ref, _}
               }
             }
           } = :sys.get_state(c.compactor)

    assert nil == Map.get(entries, file1)
    assert %{id: ^file2, priority: 2} = Map.get(entries, file2)

    send(c.compactor, {:fake_ref, {:ok, [file2], 1, 2}})

    assert %{
             levels: entries
           } = :sys.get_state(c.compactor)

    assert map_size(entries) == 0
  end

  test "multiple entries in same level are compacted together", c do
    file1 = write_sst(c.tmp_dir, "foo1", 0, 10, [{0, 1, :v1}])
    file2 = write_sst(c.tmp_dir, "foo2", 0, 10, [{1, 2, :v2}])
    file3 = write_sst(c.tmp_dir, "foo3", 0, 10, [{2, 3, :v3}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)
    %{size: size3} = File.stat!(file3)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 1}})
    assert :ok == Compactor.put(c.registry, 0, {file2, 1, size2, {2, 2}})
    assert :ok == Compactor.put(c.registry, 0, {file3, 2, size3, {3, 3}})

    assert_eventually do
      assert %{
               levels: %{
                 2 => %Level{
                   compacting_ref: nil,
                   entries: entries
                 }
               }
             } = :sys.get_state(c.compactor)

      assert [%{id: file, key_range: {1, 3}}] = Map.values(entries)

      assert [
               {0, 1, :v1},
               {1, 2, :v2},
               {2, 3, :v3}
             ] == file |> Goblin.SSTs.stream!() |> Enum.to_list()
    end
  end

  test "compaction with overlapping keys keeps highest sequence", c do
    file1 = write_sst(c.tmp_dir, "overlap1", 0, 10, [{0, 1, :v1}, {1, 2, :v2}])
    file2 = write_sst(c.tmp_dir, "overlap2", 0, 10, [{2, 1, :v1_new}, {3, 3, :v3}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 2}})
    assert :ok == Compactor.put(c.registry, 0, {file2, 2, size2, {1, 3}})

    assert_eventually do
      assert %{
               levels: %{
                 2 => %Level{
                   compacting_ref: nil,
                   entries: entries
                 }
               }
             } = :sys.get_state(c.compactor)

      assert [%{id: file}] = Map.values(entries)

      assert [
               {2, 1, :v1_new},
               {1, 2, :v2},
               {3, 3, :v3}
             ] == file |> Goblin.SSTs.stream!() |> Enum.to_list()
    end
  end

  test "compaction removes tombstones in final level", c do
    file1 = write_sst(c.tmp_dir, "tomb_final1", 0, 10, [{0, 1, :v1}, {1, 2, :tombstone}])
    file2 = write_sst(c.tmp_dir, "tomb_final2", 0, 10, [{2, 3, :v3}])
    file3 = write_sst(c.tmp_dir, "tomb_final3", 0, 10, [{3, 4, :v4}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)
    %{size: size3} = File.stat!(file3)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 2}})
    assert :ok == Compactor.put(c.registry, 0, {file2, 2, size2, {3, 3}})
    assert :ok == Compactor.put(c.registry, 0, {file3, 3, size3, {4, 4}})

    assert_eventually do
      state = :sys.get_state(c.compactor)
      assert %{levels: levels} = state
      assert map_size(levels) > 0

      final_level_key = levels |> Map.keys() |> Enum.max()
      final_level = Map.get(levels, final_level_key)
      assert final_level.compacting_ref == nil

      assert [%{id: file}] = Map.values(final_level.entries)

      data = file |> Goblin.SSTs.stream!() |> Enum.to_list()
      assert [{0, 1, :v1}, {2, 3, :v3}, {3, 4, :v4}] == data
    end
  end

  test "level limit grows exponentially with level key", c do
    state = :sys.get_state(c.compactor)
    assert state.level_limit == 100

    file1 = write_sst(c.tmp_dir, "exp1", 0, 10, [{0, 1, :v1}])
    %{size: size1} = File.stat!(file1)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 1}})

    assert_eventually do
      assert %{levels: %{1 => %Level{}}} = :sys.get_state(c.compactor)
    end

    file2 = write_sst(c.tmp_dir, "exp2", 1, 10, Enum.map(1..100, &{&1, &1, :v}))
    %{size: size2} = File.stat!(file2)

    assert :ok == Compactor.put(c.registry, 1, {file2, 0, size2, {1, 100}})

    assert_eventually do
      assert %{levels: %{2 => %Level{}}} = :sys.get_state(c.compactor)
    end
  end

  test "compaction handles empty target level", c do
    file1 = write_sst(c.tmp_dir, "empty_target1", 0, 10, [{0, 1, :v1}])
    file2 = write_sst(c.tmp_dir, "empty_target2", 0, 10, [{1, 2, :v2}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 1}})
    assert :ok == Compactor.put(c.registry, 0, {file2, 1, size2, {2, 2}})

    assert_eventually do
      state = :sys.get_state(c.compactor)
      assert %{levels: levels} = state
      assert map_size(levels) > 0

      final_level_key = levels |> Map.keys() |> Enum.max()
      final_level = Map.get(levels, final_level_key)
      assert final_level.compacting_ref == nil

      assert [%{id: file}] = Map.values(final_level.entries)
      assert [{0, 1, :v1}, {1, 2, :v2}] == file |> Goblin.SSTs.stream!() |> Enum.to_list()
    end
  end

  test "multiple compactions can happen on different level pairs", c do
    file1 = write_sst(c.tmp_dir, "multi1", 0, 10, [{0, 1, :v1}])
    file2 = write_sst(c.tmp_dir, "multi2", 2, 10, Enum.map(1..100, &{&1, &1, :v}))
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 1}})

    assert_eventually do
      assert %{levels: %{1 => %Level{}}} = :sys.get_state(c.compactor)
    end

    assert :ok == Compactor.put(c.registry, 2, {file2, 0, size2, {1, 100}})

    assert_eventually do
      assert %{
               levels: %{
                 1 => %Level{compacting_ref: nil},
                 3 => %Level{compacting_ref: nil}
               }
             } = :sys.get_state(c.compactor)
    end
  end

  @tag db_opts: [task_mod: FakeTask]
  test "handle_info ignores unknown messages", c do
    file1 = write_sst(c.tmp_dir, "ignore", 1, 10, [{0, 1, :v1}])
    %{size: size1} = File.stat!(file1)

    assert :ok == Compactor.put(c.registry, 1, {file1, 0, size1, {1, 1}})

    send(c.compactor, :unknown_message)
    send(c.compactor, {:unknown, :tuple})
    send(c.compactor, {:DOWN, make_ref(), :process, self(), :normal})

    assert %{
             levels: %{
               1 => %Level{compacting_ref: {:fake_ref, _}}
             }
           } = :sys.get_state(c.compactor)
  end

  test "compaction updates both source and target levels", c do
    file1 = write_sst(c.tmp_dir, "update1", 0, 10, [{0, 1, :v1}])
    file2 = write_sst(c.tmp_dir, "update2", 0, 10, [{1, 2, :v2}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)

    assert :ok == Compactor.put(c.registry, 0, {file1, 0, size1, {1, 1}})
    assert :ok == Compactor.put(c.registry, 0, {file2, 1, size2, {2, 2}})

    assert_eventually do
      assert %{
               levels: %{
                 2 => %Level{
                   compacting_ref: nil,
                   entries: entries
                 }
               }
             } = :sys.get_state(c.compactor)

      assert map_size(entries) == 1
    end
  end

  test "compaction with multiple files in target level", c do
    file1 = write_sst(c.tmp_dir, "target1", 1, 10, [{0, 1, :v1}])
    file2 = write_sst(c.tmp_dir, "target2", 1, 10, [{1, 10, :v10}])
    file3 = write_sst(c.tmp_dir, "source1", 0, 10, [{2, 5, :v5}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)
    %{size: size3} = File.stat!(file3)

    assert :ok == Compactor.put(c.registry, 1, {file1, 0, size1, {1, 1}})
    assert :ok == Compactor.put(c.registry, 1, {file2, 1, size2, {10, 10}})

    assert_eventually do
      assert %{levels: %{1 => %Level{entries: entries}}} = :sys.get_state(c.compactor)
      assert map_size(entries) == 2
    end

    assert :ok == Compactor.put(c.registry, 0, {file3, 2, size3, {5, 5}})

    assert_eventually do
      assert %{
               levels: %{
                 2 => %Level{
                   compacting_ref: nil,
                   entries: entries
                 }
               }
             } = :sys.get_state(c.compactor)

      assert map_size(entries) >= 1
    end
  end

  test "put/4 with different priorities maintains order", c do
    assert :ok == Compactor.put(c.registry, 0, {"high_prio", 100, 10, {1, 10}})
    assert :ok == Compactor.put(c.registry, 0, {"low_prio", 50, 10, {11, 20}})

    assert %{
             levels: %{
               0 => %Level{
                 entries: entries
               }
             }
           } = :sys.get_state(c.compactor)

    assert %{priority: 100} = Map.get(entries, "high_prio")
    assert %{priority: 50} = Map.get(entries, "low_prio")
  end

  @tag db_opts: [task_mod: FakeTask]
  test "is_compacting/1 returns true when compacting, false otherwise", c do
    refute Compactor.is_compacting(c.registry)

    assert :ok == Compactor.put(c.registry, 0, {"foo", 0, 50, {2, 3}})
    assert :ok == Compactor.put(c.registry, 0, {"bar", 1, 50, {5, 7}})

    assert Compactor.is_compacting(c.registry)
  end
end
