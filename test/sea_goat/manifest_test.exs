defmodule SeaGoat.ManifestTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias SeaGoat.Manifest

  @manifest_id :manifest_test_id
  @manifest_file "manifest.test"
  @manifest_name :manifest_test_log
  @manifest_max_size 1024
  @moduletag :tmp_dir

  setup c do
    manifest = start_manifest(c.tmp_dir)
    %{manifest: manifest, manifest_file: Path.join(c.tmp_dir, @manifest_file)}
  end

  test "adds snapshot of new version on start", c do
    assert {:ok, [snapshot: %{count: 0, seq: 0, files: MapSet.new()}]} ==
             read_disk_log(:ro_manifest, c.manifest_file)
  end

  test "recovers previous version if old file exists", c do
    assert :ok == Manifest.log_file_added(c.manifest, "foo1.seagoat")
    File.cp!(c.manifest_file, c.manifest_file <> ".old")
    assert :ok == Manifest.log_file_added(c.manifest, "foo2.seagoat")

    assert %{files: ["foo1.seagoat", "foo2.seagoat"], count: 2, seq: 0} ==
             Manifest.get_version(c.manifest)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)

    assert {:ok, [snapshot: %{count: 0, seq: 0, files: MapSet.new()}, file_added: "foo1.seagoat"]} ==
             read_disk_log(:ro_manifest, c.manifest_file)

    assert %{files: ["foo1.seagoat"], count: 1, seq: 0} == Manifest.get_version(manifest)
  end

  test "recovers version on restart", c do
    assert :ok == Manifest.log_file_added(c.manifest, "foo.seagoat")
    stop_manifest()
    manifest = start_manifest(c.tmp_dir)

    assert {:ok, [snapshot: %{count: 0, seq: 0, files: MapSet.new()}, file_added: "foo.seagoat"]} ==
             read_disk_log(:ro_manifest, c.manifest_file)

    assert %{files: ["foo.seagoat"], count: 1, seq: 0} == Manifest.get_version(manifest)
  end

  test "log_file_added/2 appends a :file_added edit to manifest and returns :ok", c do
    assert {:ok, [snapshot: _]} = read_disk_log(:ro_manifest, c.manifest_file)
    assert :ok == Manifest.log_file_added(c.manifest, "foo.seagoat")

    assert {:ok, [_, file_added: "foo.seagoat"]} =
             read_disk_log(:ro_manifest, c.manifest_file)
  end

  test "log_file_removed/2 appends a :file_removed edit to manifest and returns :ok", c do
    assert {:ok, [snapshot: _]} = read_disk_log(:ro_manifest, c.manifest_file)
    assert :ok == Manifest.log_file_removed(c.manifest, "foo.seagoat")

    assert {:ok, [_, file_removed: "foo.seagoat"]} =
             read_disk_log(:ro_manifest, c.manifest_file)
  end

  test "log_compaction/3 appends :file_added and :filed_removed edits to manifest and returns :ok",
       c do
    assert {:ok, [snapshot: _]} = read_disk_log(:ro_manifest, c.manifest_file)

    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.seagoat", "foo2.seagoat"], [
               "bar1.seagoat",
               "bar2.seagoat"
             ])

    assert {:ok,
            [
              _,
              file_added: "bar1.seagoat",
              file_added: "bar2.seagoat",
              file_removed: "foo1.seagoat",
              file_removed: "foo2.seagoat"
            ]} = read_disk_log(:ro_manifest, c.manifest_file)
  end

  test "edits are logged sequentially", c do
    assert :ok == Manifest.log_file_added(c.manifest, "foo1.seagoat")
    assert :ok == Manifest.log_file_removed(c.manifest, "bar1.seagoat")
    assert :ok == Manifest.log_file_added(c.manifest, "foo2.seagoat")

    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.seagoat", "foo2.seagoat"], [
               "bar2.seagoat",
               "bar3.seagoat"
             ])

    assert {:ok,
            [
              _,
              file_added: "foo1.seagoat",
              file_removed: "bar1.seagoat",
              file_added: "foo2.seagoat",
              file_added: "bar2.seagoat",
              file_added: "bar3.seagoat",
              file_removed: "foo1.seagoat",
              file_removed: "foo2.seagoat"
            ]} =
             read_disk_log(:ro_manifest, c.manifest_file)
  end

  test "edits update manifest version", c do
    assert %{files: [], count: 0, seq: 0} == Manifest.get_version(c.manifest)
    assert :ok == Manifest.log_file_added(c.manifest, "foo.seagoat")
    assert %{files: ["foo.seagoat"], count: 1, seq: 0} == Manifest.get_version(c.manifest)
    assert :ok == Manifest.log_file_removed(c.manifest, "foo.seagoat")
    assert %{files: [], count: 1, seq: 0} == Manifest.get_version(c.manifest)
    assert :ok == Manifest.log_sequence(c.manifest, 5)
    assert %{files: [], count: 1, seq: 5} == Manifest.get_version(c.manifest)

    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.seagoat", "foo2.seagoat"], [
               "bar1.seagoat",
               "bar2.seagoat"
             ])

    assert %{files: ["bar1.seagoat", "bar2.seagoat"], count: 3, seq: 5} ==
             Manifest.get_version(c.manifest)

    assert %{files: ["bar1.seagoat", "bar2.seagoat"]} ==
             Manifest.get_version(c.manifest, [:files])

    assert %{count: 3} == Manifest.get_version(c.manifest, [:count])
    assert %{count: 3, seq: 5} == Manifest.get_version(c.manifest, [:count, :seq])
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
