defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper
  use Mimic
  alias Goblin.Manifest
  import ExUnit.CaptureLog

  setup_db()

  test "durably tracks metadata across restarts", c do
    # can update state
    assert :ok ==
             Manifest.update(c.manifest,
               add_disk_tables: ["foo", "bar"],
               remove_disk_tables: ["baz"],
               add_wals: ["foo.wal", "bar.wal"],
               remove_wals: ["baz.wal"],
               create_wal: "wal.wal",
               seq: 999
             )

    # can get correct state
    assert %{
             disk_tables: disk_tables,
             wal_rotations: wal_rotations,
             wal: wal,
             seq: 999,
             count: 2
           } = Manifest.snapshot(c.manifest, [:disk_tables, :wal_rotations, :wal, :seq, :count])

    assert Enum.sort(["foo", "bar", "foo.wal", "bar.wal", "wal.wal"]) ==
             Enum.map([wal] ++ wal_rotations ++ disk_tables, &Path.basename/1)
             |> Enum.sort()

    # can get same state after restart
    stop_db(name: __MODULE__)
    %{manifest: manifest} = start_db(data_dir: c.tmp_dir, name: __MODULE__)

    assert %{
             disk_tables: disk_tables,
             wal_rotations: wal_rotations,
             wal: wal,
             seq: 999,
             count: 2
           } = Manifest.snapshot(manifest, [:disk_tables, :wal_rotations, :wal, :seq, :count])

    assert Enum.sort(["foo", "bar", "foo.wal", "bar.wal", "wal.wal"]) ==
             Enum.map([wal] ++ wal_rotations ++ disk_tables, &Path.basename/1)
             |> Enum.sort()
  end

  test "failing to append to log stops the server", %{manifest: manifest} do
    Process.monitor(manifest)
    Process.flag(:trap_exit, true)

    Goblin.Manifest.Log
    |> expect(:append, fn _log, _terms ->
      {:error, :failed_to_append}
    end)

    Goblin.Manifest.Log
    |> allow(self(), manifest)

    with_log(fn ->
      assert {:error, :failed_to_append} = Manifest.update(manifest, seq: 1)
      assert_receive {:DOWN, _ref, :process, ^manifest, :failed_to_append}
    end)
  end

  test "recovers from interrupted rotation", c do
    # setup interrupted rotation
    Manifest.update(c.manifest, seq: 999)
    %{log_file: log_file} = :sys.get_state(c.manifest)
    File.cp!(log_file, "#{log_file}.rotation")

    # change state
    Manifest.update(c.manifest, seq: 1000)

    # restart database
    stop_db(name: __MODULE__)
    %{manifest: manifest} = start_db(data_dir: c.tmp_dir, name: __MODULE__)

    # recover previous state
    assert %{seq: 999} == Manifest.snapshot(manifest, [:seq])
  end
end
