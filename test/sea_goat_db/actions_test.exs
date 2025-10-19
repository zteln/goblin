defmodule SeaGoatDB.ActionsTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoatDB.Actions
  alias SeaGoatDB.SSTs

  @moduletag :tmp_dir
  @db_opts [
    id: :actions_test,
    name: :actions_test,
    wal_name: :actions_test_wal_log,
    manifest_name: :actions_test_log_name
  ]

  setup_db(@db_opts)

  test "flush/3 writes an SST file on disk", c do
    rotated_wal = Path.join(c.tmp_dir, "rot_wal")
    File.touch(rotated_wal)
    data = [{0, :k1, :v1}, {1, :k2, :v2}]

    assert {:ok, :flushed} == Actions.flush(data, rotated_wal, {c.store, c.wal, c.manifest})

    refute File.exists?(rotated_wal)

    assert %{ss_tables: [%{file: flushed_file}]} = :sys.get_state(c.store)

    assert [{0, :k1, :v1}, {1, :k2, :v2}] == SSTs.stream!(flushed_file) |> Enum.to_list()
  end

  test "merge/7 merges multiple SST files into new SST files", c do
    file1 = write_sst(c.tmp_dir, "foo", 0, [{0, :k1, :v1}, {1, :k2, :v2}])
    file2 = write_sst(c.tmp_dir, "bar", 0, [{2, :k3, :v3}, {3, :k4, :v4}])
    file3 = write_sst(c.tmp_dir, "baz", 0, [{4, :k5, :v5}, {5, :k6, :v6}])
    for file <- [file1, file2, file3], do: assert(File.exists?(file))

    # Create a target level using the proper Level struct
    target_level = %SeaGoatDB.Compactor.Level{
      level_key: 1,
      entries: %{},
      compacting_ref: nil
    }

    files = [file1, file2, file3]

    assert {:ok, old_files, 0, 1} =
             Actions.merge(
               files,
               0,
               target_level,
               &SeaGoatDB.Compactor.Level.place_in_buffer(&2, &1),
               false,
               1000,
               {c.store, c.manifest, c.rw_locks}
             )

    # Verify old files are returned
    assert Enum.sort(old_files) == Enum.sort(files)

    # Verify original files are removed
    for file <- files do
      refute File.exists?(file)
    end

    # Verify new files are created in the store
    assert %{ss_tables: [%{file: file}]} = :sys.get_state(c.store)
    assert file == Path.join(c.tmp_dir, "0.seagoat")

    assert [
             {0, :k1, :v1},
             {1, :k2, :v2},
             {2, :k3, :v3},
             {3, :k4, :v4},
             {4, :k5, :v5},
             {5, :k6, :v6}
           ] == file |> SSTs.stream!() |> Enum.to_list()
  end

  test "merge/7 handles overlapping keys with sequence numbers", c do
    # Same keys with different sequence numbers
    file1 = write_sst(c.tmp_dir, "foo", 0, [{5, :k1, :v1_old}, {6, :k2, :v2}])
    file2 = write_sst(c.tmp_dir, "bar", 0, [{10, :k1, :v1_new}, {11, :k3, :v3}])

    target_level = %SeaGoatDB.Compactor.Level{
      level_key: 1,
      entries: %{},
      compacting_ref: nil
    }

    assert {:ok, _old_files, 0, 1} =
             Actions.merge(
               [file1, file2],
               0,
               target_level,
               &SeaGoatDB.Compactor.Level.place_in_buffer(&2, &1),
               false,
               1000,
               {c.store, c.manifest, c.rw_locks}
             )

    # Verify the merged data contains the newer value for k1
    %{ss_tables: [%{file: merged_file}]} = :sys.get_state(c.store)
    merged_data = SSTs.stream!(merged_file) |> Enum.to_list()

    # Should contain the newer k1 value and all other keys
    k1_entries = Enum.filter(merged_data, fn {_seq, key, _val} -> key == :k1 end)
    assert [{10, :k1, :v1_new}] = k1_entries
  end

  test "merge/7 removes tombstones when clean_tombstones? is true", c do
    file1 = write_sst(c.tmp_dir, "foo", 0, [{1, :k1, :v1}, {2, :k2, :tombstone}])
    file2 = write_sst(c.tmp_dir, "bar", 0, [{3, :k3, :v3}, {4, :k4, :tombstone}])

    target_level = %SeaGoatDB.Compactor.Level{
      level_key: 1,
      entries: %{},
      compacting_ref: nil
    }

    # Test with clean_tombstones? = true
    assert {:ok, _old_files, 0, 1} =
             Actions.merge(
               [file1, file2],
               0,
               target_level,
               &SeaGoatDB.Compactor.Level.place_in_buffer(&2, &1),
               true,
               1000,
               {c.store, c.manifest, c.rw_locks}
             )

    %{ss_tables: [%{file: merged_file}]} = :sys.get_state(c.store)
    merged_data = SSTs.stream!(merged_file) |> Enum.to_list()

    # Should only contain non-tombstone entries
    assert length(merged_data) == 2
    refute Enum.any?(merged_data, fn {_seq, _key, val} -> val == :tombstone end)
    assert Enum.any?(merged_data, fn {_seq, key, _val} -> key == :k1 end)
    assert Enum.any?(merged_data, fn {_seq, key, _val} -> key == :k3 end)
  end

  test "merge/7 keeps tombstones when clean_tombstones? is false", c do
    file1 = write_sst(c.tmp_dir, "foo", 0, [{1, :k1, :v1}, {2, :k2, :tombstone}])

    target_level = %SeaGoatDB.Compactor.Level{
      level_key: 1,
      entries: %{},
      compacting_ref: nil
    }

    assert {:ok, _old_files, 0, 1} =
             Actions.merge(
               [file1],
               0,
               target_level,
               &SeaGoatDB.Compactor.Level.place_in_buffer(&2, &1),
               false,
               1000,
               {c.store, c.manifest, c.rw_locks}
             )

    %{ss_tables: [%{file: merged_file}]} = :sys.get_state(c.store)
    merged_data = SSTs.stream!(merged_file) |> Enum.to_list()

    # Should contain both regular entry and tombstone
    assert length(merged_data) == 2
    assert Enum.any?(merged_data, fn {_seq, _key, val} -> val == :tombstone end)

    assert Enum.any?(merged_data, fn
             {2, :k2, :tombstone} -> true
             _ -> false
           end)
  end

  test "merge/7 splits files when key_limit is exceeded", c do
    # Create enough data to exceed key limit
    large_data = for i <- 1..50, do: {i, String.to_atom("k#{i}"), "v#{i}"}
    file1 = write_sst(c.tmp_dir, "foo", 0, large_data)

    target_level = %SeaGoatDB.Compactor.Level{
      level_key: 1,
      entries: %{},
      compacting_ref: nil
    }

    # Use small key_limit to force splitting
    assert {:ok, _old_files, 0, 1} =
             Actions.merge(
               [file1],
               0,
               target_level,
               &SeaGoatDB.Compactor.Level.place_in_buffer(&2, &1),
               false,
               # Small key limit
               10,
               {c.store, c.manifest, c.rw_locks}
             )

    # Should create multiple files due to key limit
    %{ss_tables: ss_tables} = :sys.get_state(c.store)
    assert length(ss_tables) == 5

    # Verify all data is preserved across files
    all_data =
      ss_tables
      |> Enum.flat_map(fn %{file: file} ->
        file
        |> SSTs.stream!()
        |> Enum.to_list()
      end)
      |> Enum.sort()

    expected_data = Enum.sort(large_data)
    assert all_data == expected_data
  end

  test "merge/7 waits until wlock completes to delete merged files", c do
    parent = self()
    ref = make_ref()
    large_data = for i <- 1..50, do: {i, String.to_atom("k#{i}"), "v#{i}"}
    file = write_sst(c.tmp_dir, "foo", 0, large_data)

    target_level = %SeaGoatDB.Compactor.Level{
      level_key: 1,
      entries: %{},
      compacting_ref: nil
    }

    reader_pid =
      spawn(fn ->
        SeaGoatDB.RWLocks.rlock(c.rw_locks, file)

        send(parent, {:rlocked, ref})

        receive do
          {:unlock, ^ref} -> SeaGoatDB.RWLocks.unlock(c.rw_locks, file)
        end
      end)

    receive do
      {:rlocked, ^ref} -> :ok
    end

    merger_pid =
      spawn(fn ->
        assert {:ok, _old_files, 0, 1} =
                 Actions.merge(
                   [file],
                   0,
                   target_level,
                   &SeaGoatDB.Compactor.Level.place_in_buffer(&2, &1),
                   false,
                   # Small key limit
                   10,
                   {c.store, c.manifest, c.rw_locks}
                 )

        send(parent, {:merge_completed, ref})
      end)

    assert_eventually do
      assert %{
               locks: %{
                 ^file => %{
                   waiting: {:wlock, {^merger_pid, _, _}},
                   current: [rlock: {^reader_pid, _, _}]
                 }
               }
             } = :sys.get_state(c.rw_locks)
    end

    assert File.exists?(file)
    send(reader_pid, {:unlock, ref})

    receive do
      {:merge_completed, ^ref} ->
        refute File.exists?(file)
    end
  end
end
