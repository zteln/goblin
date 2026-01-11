defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog

  @moduletag :tmp_dir
  setup_db()

  test "can update database state", c do
    assert %{disk_tables: [], wal_rotations: [], wal: _wal, count: 0, seq: 0} =
             Goblin.Manifest.snapshot(c.manifest, [
               :disk_tables,
               :wal,
               :wal_rotations,
               :count,
               :seq
             ])

    assert :ok == Goblin.Manifest.log_wal(c.manifest, "new_wal")
    assert %{wal: Path.join(c.tmp_dir, "new_wal")} == Goblin.Manifest.snapshot(c.manifest, [:wal])

    assert :ok == Goblin.Manifest.log_rotation(c.manifest, "wal1", "wal2")

    assert %{wal_rotations: [Path.join(c.tmp_dir, "wal1")], wal: Path.join(c.tmp_dir, "wal2")} ==
             Goblin.Manifest.snapshot(c.manifest, [:wal, :wal_rotations])

    assert :ok == Goblin.Manifest.log_flush(c.manifest, ["sst1"], "wal1")

    assert %{wal_rotations: [], disk_tables: [Path.join(c.tmp_dir, "sst1")]} ==
             Goblin.Manifest.snapshot(c.manifest, [:disk_tables, :wal_rotations])

    assert :ok == Goblin.Manifest.log_sequence(c.manifest, 5)
    assert %{seq: 5} == Goblin.Manifest.snapshot(c.manifest, [:seq])

    assert :ok == Goblin.Manifest.log_compaction(c.manifest, ["sst1"], ["sst2"])

    assert %{disk_tables: [Path.join(c.tmp_dir, "sst2")]} ==
             Goblin.Manifest.snapshot(c.manifest, [:disk_tables])
  end

  @tag db_opts: [manifest_max_size: 512]
  test "automatically rotates when exceeding size limit", c do
    %{file: file} = :sys.get_state(c.manifest)
    %{size: size} = File.stat!(file)

    [seq] =
      Stream.iterate(1, &(&1 + 1))
      |> Stream.transform(size, fn n, acc ->
        Goblin.Manifest.log_sequence(c.manifest, n)
        %{size: size} = File.stat!(file)

        if size < acc do
          {:halt, size}
        else
          {[n], size}
        end
      end)
      |> Stream.take(-1)
      |> Enum.to_list()

    assert %{seq: seq + 1} == Goblin.Manifest.snapshot(c.manifest, [:seq])
  end

  test "recovers previous manifest if it exists", c do
    assert %{seq: 0} == Goblin.Manifest.snapshot(c.manifest, [:seq])
    %{file: file} = :sys.get_state(c.manifest)
    File.cp!(file, "#{file}.0")
    Goblin.Manifest.log_sequence(c.manifest, 1)

    {manifest, _log} =
      with_log(fn ->
        stop_db(__MODULE__)
        %{manifest: manifest} = start_db(c.tmp_dir, name: __MODULE__)
        manifest
      end)

    assert %{seq: 0} == Goblin.Manifest.snapshot(manifest, [:seq])
  end

  test "only cleans up Goblin generated files on start", c do
    other_file = Path.join(c.tmp_dir, "foo")
    File.touch!(other_file)

    goblin_file = Path.join(c.tmp_dir, "bar")
    File.touch!(goblin_file)

    Goblin.Manifest.log_compaction(c.manifest, [goblin_file], [])

    stop_db(__MODULE__)
    start_db(c.tmp_dir, name: __MODULE__)

    assert_eventually do
      assert File.exists?(other_file)
      refute File.exists?(goblin_file)
    end
  end
end
