defmodule SeaGoat.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  use Patch
  alias SeaGoat.Compactor
  alias SeaGoat.Compactor.Level
  alias SeaGoat.Compactor.Entry

  @moduletag :tmp_dir
  @db_opts [
    id: :compactor_test,
    name: :compactor_test,
    key_limit: 10,
    sync_interval: 50,
    level_limit: 100,
    wal_name: :compactor_test_wal_log,
    manifest_name: :compactor_test_log_name
  ]

  setup_db(@db_opts)

  test "compactor starts with empty levels", c do
    assert %{levels: levels} = :sys.get_state(c.compactor)
    assert %{} == levels
  end

  test "put/4 puts file in level queue for compaction", c do
    assert :ok == Compactor.put(c.compactor, 0, "foo", {0, 1, {2, 3}})

    assert %{
             levels: %{
               0 => %Level{
                 entries: %{"foo" => %Entry{id: "foo", priority: 0, size: 1, key_range: {2, 3}}}
               }
             }
           } = :sys.get_state(c.compactor)
  end

  test "level with size exceeding level_limit causes a merge", c do
    ignore_merge()

    assert :ok == Compactor.put(c.compactor, 0, "foo", {0, 50, {2, 3}})
    assert %{levels: %{0 => %Level{compacting_ref: nil}}} = :sys.get_state(c.compactor)

    assert :ok == Compactor.put(c.compactor, 0, "bar", {1, 50, {5, 7}})

    assert %{
             levels: %{
               0 => %Level{compacting_ref: ref},
               1 => %Level{compacting_ref: ref}
             }
           } = :sys.get_state(c.compactor)

    assert is_reference(ref)
  end

  test "a completed compaction cleans up sources", c do
    file1 = write_sst(c.tmp_dir, "foo", 0, [{0, 1, :v1}, {1, 2, :v2}])
    file2 = write_sst(c.tmp_dir, "bar", 0, [{2, 3, :v3}, {3, 4, :v4}])
    %{size: size1} = File.stat!(file1)
    %{size: size2} = File.stat!(file2)
    assert :ok == Compactor.put(c.compactor, 0, file1, {0, size1, {1, 2}})
    assert :ok == Compactor.put(c.compactor, 0, file2, {2, size2, {3, 4}})
    assert File.exists?(file1)
    assert File.exists?(file2)

    assert_eventually do
      assert %{
               levels: %{
                 0 => %Level{compacting_ref: nil},
                 1 => %Level{compacting_ref: nil},
                 2 => %Level{
                   compacting_ref: nil,
                   entries: entries
                 }
               }
             } = :sys.get_state(c.compactor)

      assert [%{id: file, priority: 0, size: size, key_range: {1, 4}}] = Map.values(entries)
      assert size < size1 + size2

      assert [
               {0, 1, :v1},
               {1, 2, :v2},
               {2, 3, :v3},
               {3, 4, :v4}
             ] == file |> SeaGoat.SSTables.stream!() |> Enum.to_list()
    end

    refute File.exists?(file1)
    refute File.exists?(file2)
  end

  defp ignore_merge, do: patch(SeaGoat.Actions, :merge, {:ok, :ignore})
end
