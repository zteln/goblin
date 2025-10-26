defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Manifest

  @manifest_id :manifest_test_id
  @manifest_file "manifest.goblin"
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
    assert :ok == Manifest.log_rotation(c.manifest, "wal.goblin.rot.1")
    assert %{wals: ["wal.goblin.rot.1"]} == Manifest.get_version(c.manifest, [:wals])

    assert {:ok, [_, {:wal_added, "wal.goblin.rot.1"}]} =
             read_manifest(:ro_manifest, c.manifest_file)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)

    assert %{wals: ["wal.goblin.rot.1"]} == Manifest.get_version(manifest, [:wals])
  end

  test "log_flush/3 adds new file to files set and removes wal from wals set", c do
    wal_file = "wal.goblin.rot.1"
    sst_file = "0.goblin"
    assert :ok == Manifest.log_rotation(c.manifest, wal_file)
    assert :ok == Manifest.log_flush(c.manifest, [sst_file], wal_file)

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
    rotated_files = ["0.goblin", "1.goblin"]
    new_files = ["2.goblin", "3.goblin"]

    assert :ok == Manifest.log_compaction(c.manifest, rotated_files, new_files)
    assert %{files: ["2.goblin", "3.goblin"]} == Manifest.get_version(c.manifest, [:files])

    assert {:ok,
            [
              _,
              {:file_added, "2.goblin"},
              {:file_added, "3.goblin"},
              {:file_removed, "0.goblin"},
              {:file_removed, "1.goblin"}
            ]} = read_manifest(:ro_manifest, c.manifest_file)

    stop_manifest()
    manifest = start_manifest(c.tmp_dir)
    assert %{files: ["2.goblin", "3.goblin"]} == Manifest.get_version(manifest, [:files])
  end

  test "log file rotates when it exceeds size limit", c do
    assert {:ok, [snapshot: %{count: 0, seq: 0, files: MapSet.new(), wals: MapSet.new()}]} ==
             read_manifest(:ro_manifest, c.manifest_file)

    for _ <- 1..10 do
      assert :ok == Manifest.log_compaction(c.manifest, ["foo.goblin"], ["bar.goblin"])
    end

    assert {:ok,
            [
              snapshot: %{
                count: 9,
                seq: 0,
                files: MapSet.new(["bar.goblin"]),
                wals: MapSet.new()
              },
              file_added: "bar.goblin",
              file_removed: "foo.goblin"
            ]} == read_manifest(:ro_manifest, c.manifest_file)
  end

  test "edits are logged sequentially", c do
    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.goblin", "foo2.goblin"], [
               "bar2.goblin",
               "bar3.goblin"
             ])

    assert :ok ==
             Manifest.log_compaction(c.manifest, ["foo1.goblin", "foo2.goblin"], [
               "bar2.goblin",
               "bar3.goblin"
             ])

    assert {:ok,
            [
              _,
              file_added: "bar2.goblin",
              file_added: "bar3.goblin",
              file_removed: "foo1.goblin",
              file_removed: "foo2.goblin",
              file_added: "bar2.goblin",
              file_added: "bar3.goblin",
              file_removed: "foo1.goblin",
              file_removed: "foo2.goblin"
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
