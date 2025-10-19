defmodule SeaGoat.ManifestTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoat.Manifest

  @manifest_id :manifest_test_id
  @manifest_file "manifest.seagoat"
  @manifest_name :manifest_test_log
  @manifest_max_size 512
  @moduletag :tmp_dir

  setup c do
    manifest = start_manifest(c.tmp_dir)
    %{manifest: manifest, manifest_file: Path.join(c.tmp_dir, @manifest_file)}
  end

  test "adds snapshot of new version on start", c do
    assert {:ok, [snapshot: %{count: 0, seq: 0, files: MapSet.new(), wals: MapSet.new()}]} ==
             read_manifest(:ro_manifest, c.manifest_file)
  end

  test "log_rotation/2 adds rotated wal file to wals set", c do
    assert :ok == Manifest.log_rotation(c.manifest, "wal.seagoat.rot.1")
    assert %{wals: ["wal.seagoat.rot.1"]} == Manifest.get_version(c.manifest, [:wals])

    assert {:ok, [_, {:wal_added, "wal.seagoat.rot.1"}]} =
             read_manifest(:ro_manifest, c.manifest_file)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)

    assert %{wals: ["wal.seagoat.rot.1"]} == Manifest.get_version(manifest, [:wals])
  end

  test "log_flush/3 adds new file to files set and removes wal from wals set", c do
    wal_file = "wal.seagoat.rot.1"
    sst_file = "0.seagoat"
    assert :ok == Manifest.log_rotation(c.manifest, wal_file)
    assert :ok == Manifest.log_flush(c.manifest, sst_file, wal_file)

    assert %{wals: [], files: [sst_file]} == Manifest.get_version(c.manifest, [:wals, :files])

    assert {:ok,
            [_, {:wal_added, ^wal_file}, {:file_added, ^sst_file}, {:wal_removed, ^wal_file}]} =
             read_manifest(:ro_manifest, c.manifest_file)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)
    assert %{wals: [], files: [sst_file]} == Manifest.get_version(manifest, [:wals, :files])
  end

  test "log_sequence/2 adds sequence count to manifest", c do
    sequence = Enum.random(1..100)
    assert :ok == Manifest.log_sequence(c.manifest, sequence)
    assert %{seq: sequence} == Manifest.get_version(c.manifest, [:seq])

    assert {:ok, [_, {:seq, sequence}]} = read_manifest(:ro_manifest, c.manifest_file)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)
    assert %{seq: sequence} == Manifest.get_version(manifest, [:seq])
  end

  test "log_compaction/3 adds new files and removes old files", c do
    rotated_files = ["0.seagoat", "1.seagoat"]
    new_files = ["2.seagoat", "3.seagoat"]

    assert :ok == Manifest.log_compaction(c.manifest, rotated_files, new_files)
    assert %{files: ["2.seagoat", "3.seagoat"]} == Manifest.get_version(c.manifest, [:files])

    assert {:ok,
            [
              _,
              {:file_added, "2.seagoat"},
              {:file_added, "3.seagoat"},
              {:file_removed, "0.seagoat"},
              {:file_removed, "1.seagoat"}
            ]} = read_manifest(:ro_manifest, c.manifest_file)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)
    assert %{files: ["2.seagoat", "3.seagoat"]} == Manifest.get_version(manifest, [:files])
  end

  test "log file rotates when it exceeds size limit", c do
    assert {:ok, [snapshot: %{count: 0, seq: 0, files: MapSet.new(), wals: MapSet.new()}]} ==
             read_manifest(:ro_manifest, c.manifest_file)

    for _ <- 1..9 do
      assert :ok == Manifest.log_compaction(c.manifest, ["foo.seagoat"], ["bar.seagoat"])
    end

    assert {:ok,
            [
              snapshot: %{
                count: 8,
                seq: 0,
                files: MapSet.new(["bar.seagoat"]),
                wals: MapSet.new()
              },
              file_added: "bar.seagoat",
              file_removed: "foo.seagoat"
            ]} == read_manifest(:ro_manifest, c.manifest_file)
  end

  test "edits are logged sequentially", c do
    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.seagoat", "foo2.seagoat"], [
               "bar2.seagoat",
               "bar3.seagoat"
             ])

    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.seagoat", "foo2.seagoat"], [
               "bar2.seagoat",
               "bar3.seagoat"
             ])

    assert {:ok,
            [
              _,
              file_added: "bar2.seagoat",
              file_added: "bar3.seagoat",
              file_removed: "foo1.seagoat",
              file_removed: "foo2.seagoat",
              file_added: "bar2.seagoat",
              file_added: "bar3.seagoat",
              file_removed: "foo1.seagoat",
              file_removed: "foo2.seagoat",
            ]} =
             read_manifest(:ro_manifest, c.manifest_file)
  end

  def read_manifest(name, file) do
    with {:ok, log} <- :disk_log.open(name: name, file: ~c"#{file}", mode: :read_only),
         {:ok, content} <- do_manifest(log),
         :ok <- :disk_log.close(log) do
      {:ok, content}
    end
  end

  defp do_manifest(log, continuation \\ :start, acc \\ []) do
    case :disk_log.chunk(log, continuation) do
      {:error, _} = error ->
        error

      :eof ->
        {:ok, acc}

      {continuation, chunk} ->
        do_manifest(log, continuation, acc ++ chunk)
    end
  end

  defp start_manifest(dir) do
    start_link_supervised!(
      {Manifest,
       db_dir: dir,
       manifest_log_name: @manifest_name,
       manifest_file: @manifest_file,
       manifest_max_size: @manifest_max_size},
      id: @manifest_id
    )
  end

  defp stop_manifest, do: stop_supervised(@manifest_id)
end
