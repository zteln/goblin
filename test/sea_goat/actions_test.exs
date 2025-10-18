defmodule SeaGoat.ActionsTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoat.Actions
  alias SeaGoat.SSTables

  @moduletag :tmp_dir
  @db_opts [
    id: :flusher_test,
    name: :flusher_test,
    wal_name: :flusher_test_wal_log,
    manifest_name: :flusher_test_log_name
  ]

  setup_db(@db_opts)

  test "flush/3 writes an SST file on disk", c do
    rotated_wal = Path.join(c.tmp_dir, "rot_wal")
    File.touch(rotated_wal)
    data = [{0, :k1, :v1}, {1, :k2, :v2}]

    assert {:ok, :flushed} == Actions.flush(data, rotated_wal, {c.store, c.wal, c.manifest})

    refute File.exists?(rotated_wal)

    assert %{ss_tables: [%{file: flushed_file}]} = :sys.get_state(c.store)

    assert [{0, :k1, :v1}, {1, :k2, :v2}] == SSTables.stream(flushed_file) |> Enum.to_list()
  end
end
