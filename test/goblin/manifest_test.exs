defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Manifest

  @manifest_max_size 512
  @moduletag :tmp_dir

  describe "start_link/1" do
    test "creates log file", c do
      assert 0 == length(File.ls!(c.tmp_dir))
      assert {:ok, _manifest} = Manifest.start_link(name: __MODULE__, db_dir: c.tmp_dir)
      assert 1 == length(File.ls!(c.tmp_dir))
    end

    test "fails if given a non-existing dir", c do
      Process.flag(:trap_exit, true)
      dir = Path.join(c.tmp_dir, "foo")

      assert {:error, {:file_error, _, :enoent}} =
               Manifest.start_link(name: __MODULE__, db_dir: dir)
    end

    test "fails if already started", c do
      Process.flag(:trap_exit, true)
      assert {:ok, manifest} = Manifest.start_link(name: __MODULE__, db_dir: c.tmp_dir)

      assert {:error, {:already_started, ^manifest}} =
               Manifest.start_link(name: __MODULE__, db_dir: c.tmp_dir)
    end
  end

  describe "log_rotation/2, log_flush/3, log_sequence/2, log_compaction/3, get_version/2" do
    setup c, do: start_manifest(c.tmp_dir)

    test "updates manifest version", c do
      assert %{files: [], wals: [], seq: 0, count: 0} == Manifest.get_version(c.manifest)

      assert :ok == Manifest.log_rotation(c.manifest, "foo")

      assert %{files: [], wals: ["foo"], seq: 0, count: 0} == Manifest.get_version(c.manifest)

      assert :ok == Manifest.log_flush(c.manifest, ["bar"], "foo")

      assert %{files: ["bar"], wals: [], seq: 0, count: 1} == Manifest.get_version(c.manifest)

      assert :ok == Manifest.log_sequence(c.manifest, 5)

      assert %{files: ["bar"], wals: [], seq: 5, count: 1} == Manifest.get_version(c.manifest)

      assert :ok == Manifest.log_compaction(c.manifest, ["foo1", "foo2"], ["bar1", "bar2"])

      assert %{files: ["bar", "bar1", "bar2"], wals: [], seq: 5, count: 3} == Manifest.get_version(c.manifest)
    end

    test "is persistant", c do
      assert :ok == Manifest.log_flush(c.manifest, ["bar"], "foo")

      assert %{files: ["bar"], wals: [], seq: 0, count: 1} == Manifest.get_version(c.manifest)

      stop_manifest()
      %{manifest: manifest} = start_manifest(c.tmp_dir)

      assert %{files: ["bar"], wals: [], seq: 0, count: 1} == Manifest.get_version(manifest)
    end
  end
  defp start_manifest(dir) do
    manifest =
      start_link_supervised!(
        {Manifest, db_dir: dir, name: __MODULE__, manifest_max_size: @manifest_max_size},
        id: __MODULE__
      )

    %{manifest: manifest}
  end

  defp stop_manifest, do: stop_supervised(__MODULE__)
end
