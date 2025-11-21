defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Manifest

  defmodule FailingTask do
    def async(sup, _) do
      Task.Supervisor.async(sup, fn ->
        {:error, :task_failed}
      end)
    end
  end

  defmodule WaitingTask do
    def async(sup, f) do
      Task.Supervisor.async(sup, fn ->
        receive do
          :cont -> :ok
        end

        f.()
      end)
    end
  end

  @moduletag :tmp_dir
  setup_db(manifest_max_size: 512)

  describe "on start" do
    test "starts with an initial version", c do
      assert %{
               version: %{
                 ssts: ssts,
                 wal_rotations: wal_rotations,
                 wal: wal,
                 count: 0,
                 seq: 0
               }
             } = :sys.get_state(c.manifest)

      assert MapSet.new() == ssts
      assert MapSet.new() == wal_rotations
      assert String.starts_with?(wal, c.tmp_dir)
    end

    test "cleans orphaned/corrupted files", c do
      fake_sst = Path.join(c.tmp_dir, "sst.goblin.tmp")
      {:ok, rotation, current} = Goblin.WAL.rotate(c.wal)
      Manifest.log_wal(c.manifest, current)
      File.touch(fake_sst)

      assert File.exists?(rotation)
      assert File.exists?(fake_sst)

      stop_db(__MODULE__)
      start_db(c.tmp_dir)

      refute File.exists?(rotation)
      refute File.exists?(fake_sst)
    end

    test "recovers manifest version", c do
      new_wal = Path.join(c.tmp_dir, "wal.goblin.1")
      Manifest.log_wal(c.manifest, "wal.goblin.0")
      Manifest.log_rotation(c.manifest, "wal.goblin.0", new_wal)
      Manifest.log_flush(c.manifest, ["sst1"], "wal.goblin.0")

      assert %{
               version: %{
                 ssts: ssts,
                 wal_rotations: wal_rotations,
                 wal: wal,
                 count: 1,
                 seq: 0
               }
             } = :sys.get_state(c.manifest)

      assert MapSet.new(["sst1"]) == ssts
      assert MapSet.new([]) == wal_rotations
      assert new_wal == wal

      stop_db(__MODULE__)
      %{manifest: manifest} = start_db(c.tmp_dir)

      assert %{
               version: %{
                 ssts: ssts,
                 wal_rotations: wal_rotations,
                 wal: wal,
                 count: 1,
                 seq: 0
               }
             } = :sys.get_state(manifest)

      assert MapSet.new(["sst1"]) == ssts
      assert MapSet.new([]) == wal_rotations
      assert new_wal == wal
    end
  end

  describe "log_wal/2" do
    test "puts current wal", c do
      wal = "wal.goblin.0"
      assert :ok == Manifest.log_wal(c.manifest, wal)
      assert %{wal: wal} == Manifest.get_version(c.manifest, [:wal])
    end

    test "overrides current wal", c do
      wal = "wal.goblin.0"
      assert :ok == Manifest.log_wal(c.manifest, wal)
      assert %{wal: wal} == Manifest.get_version(c.manifest, [:wal])

      wal = "wal.goblin.1"
      assert :ok == Manifest.log_wal(c.manifest, wal)
      assert %{wal: wal} == Manifest.get_version(c.manifest, [:wal])
    end
  end

  describe "log_rotation/3" do
    test "adds rotation wal and updates current wal", c do
      rotation_wal = "wal.goblin.0"
      current_wal = "wal.goblin.1"
      assert :ok == Manifest.log_rotation(c.manifest, rotation_wal, current_wal)

      assert %{wal_rotations: [rotation_wal], wal: current_wal} ==
               Manifest.get_version(c.manifest, [:wal_rotations, :wal])
    end
  end

  describe "log_flush/3" do
    test "removes rotated wal and adds sst", c do
      Manifest.log_rotation(c.manifest, "wal0", "wal1")

      assert %{wal_rotations: ["wal0"], ssts: []} ==
               Manifest.get_version(c.manifest, [:wal_rotations, :ssts])

      assert :ok == Manifest.log_flush(c.manifest, ["sst1", "sst2"], "wal0")

      assert %{wal_rotations: [], ssts: ["sst1", "sst2"]} ==
               Manifest.get_version(c.manifest, [:wal_rotations, :ssts])
    end
  end

  describe "log_sequence/2" do
    test "updates sequence number", c do
      assert %{seq: 0} == Manifest.get_version(c.manifest, [:seq])
      assert :ok == Manifest.log_sequence(c.manifest, 100)
      assert %{seq: 100} == Manifest.get_version(c.manifest, [:seq])
    end
  end

  describe "log_compaction/3" do
    test "updates SSTs", c do
      assert %{ssts: []} == Manifest.get_version(c.manifest, [:ssts])
      assert :ok == Manifest.log_compaction(c.manifest, ["sst0"], ["sst1"])
      assert %{ssts: ["sst1"]} == Manifest.get_version(c.manifest, [:ssts])
      assert :ok == Manifest.log_compaction(c.manifest, ["sst1"], ["sst2"])
      assert %{ssts: ["sst2"]} == Manifest.get_version(c.manifest, [:ssts])
    end
  end

  describe "export/2" do
    setup c do
      export_dir = Path.join(c.tmp_dir, "exports")
      File.mkdir!(export_dir)
      %{export_dir: export_dir}
    end

    test "exports a .tar.gz snapshot from manifest", c do
      assert {:ok, tar_name} = Manifest.export(c.manifest, c.export_dir)
      assert String.ends_with?(tar_name, ".tar.gz")

      {:ok, tar_content} = :erl_tar.extract(~c"#{tar_name}", [:memory, :compressed])

      Enum.each(tar_content, fn {name, content} ->
        filename = Path.join(c.tmp_dir, to_string(name))
        assert content == File.read!(filename)
      end)
    end

    @tag db_opts: [task_mod: FailingTask]
    test "returns error from task on failure", c do
      assert {:error, :task_failed} == Manifest.export(c.manifest, c.export_dir)
    end

    test "exports a copy of manifest file", c do
      %{file: manifest} = :sys.get_state(c.manifest)
      manifest_copy = "#{manifest}.testcopy"
      File.cp!(manifest, manifest_copy)

      assert {:ok, tar_name} = Manifest.export(c.manifest, c.export_dir)

      {:ok, [{_name, content}]} =
        :erl_tar.extract(~c"#{tar_name}", [:memory, :compressed, files: [~c"manifest.goblin"]])

      Manifest.log_wal(c.manifest, "wal")

      assert content == File.read!(manifest_copy)
      refute content == File.read!(manifest)
    end

    @tag db_opts: [task_mod: WaitingTask]
    test "exporting is non-blocking", c do
      parent = self()

      spawn(fn ->
        send(parent, :ready)
        assert {:ok, _tar_name} = Manifest.export(c.manifest, c.export_dir)
        send(parent, :done)
      end)

      assert_receive :ready

      assert_eventually do
        assert [{_, exporting_pid, _, _}] = Supervisor.which_children(c.task_sup)
        assert :ok == Manifest.log_wal(c.manifest, "wal")
        send(exporting_pid, :cont)
      end

      assert_receive :done
    end
  end
end
