defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true

  alias Goblin.Manifest

  @moduletag :tmp_dir

  defp manifest_name(c), do: :"#{c.test}"

  defp open_manifest(c) do
    {:ok, manifest} = Manifest.open(manifest_name(c), c.tmp_dir)
    manifest
  end

  describe "open/2" do
    test "opens a fresh manifest with empty snapshot", c do
      manifest = open_manifest(c)

      assert %{seq: 0} = Manifest.snapshot(manifest, [:seq])
      assert %{active_wal: nil} = Manifest.snapshot(manifest, [:active_wal])
      assert %{active_disk_tables: []} = Manifest.snapshot(manifest, [:active_disk_tables])
    end

    test "recovers state from an existing manifest log", c do
      manifest = open_manifest(c)
      {:ok, manifest} = Manifest.update(manifest, set_seq: 42)
      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(manifest_name(c), c.tmp_dir)

      assert %{seq: 42} = Manifest.snapshot(recovered, [:seq])
    end
  end

  describe "update/2" do
    test "persists actions and updates the snapshot", c do
      manifest = open_manifest(c)

      {:ok, manifest} =
        Manifest.update(manifest,
          set_seq: 5,
          activate_wal: "wal.goblin",
          activate_disk_tables: ["sst_0.goblin"]
        )

      assert %{seq: 5} = Manifest.snapshot(manifest, [:seq])
      assert %{active_wal: wal} = Manifest.snapshot(manifest, [:active_wal])
      assert String.ends_with?(wal, "wal.goblin")
      assert %{active_disk_tables: [dt]} = Manifest.snapshot(manifest, [:active_disk_tables])
      assert String.ends_with?(dt, "sst_0.goblin")
    end
  end

  describe "snapshot/2" do
    test "returns only requested keys with resolved paths", c do
      manifest = open_manifest(c)
      {:ok, manifest} = Manifest.update(manifest, set_seq: 10, activate_wal: "wal.goblin")

      seq_only = Manifest.snapshot(manifest, [:seq])
      assert seq_only == %{seq: 10}
      refute Map.has_key?(seq_only, :active_wal)

      wal_only = Manifest.snapshot(manifest, [:active_wal])
      assert %{active_wal: wal} = wal_only
      assert wal == Path.join(c.tmp_dir, "wal.goblin")
      refute Map.has_key?(wal_only, :seq)
    end
  end

  describe "rotate?/1" do
    test "returns false when log is small", c do
      manifest = open_manifest(c)

      refute Manifest.rotate?(manifest)
    end
  end

  describe "rotate/1" do
    test "compacts the log and preserves state", c do
      manifest = open_manifest(c)

      {:ok, manifest} = Manifest.update(manifest, set_seq: 1)
      {:ok, manifest} = Manifest.update(manifest, set_seq: 2)
      {:ok, manifest} = Manifest.update(manifest, set_seq: 3, activate_wal: "wal.goblin")

      {:ok, rotated} = Manifest.rotate(manifest)

      assert rotated.log_size > 0
      assert %{seq: 3} = Manifest.snapshot(rotated, [:seq])
      assert %{active_wal: wal} = Manifest.snapshot(rotated, [:active_wal])
      assert String.ends_with?(wal, "wal.goblin")

      # State survives close + re-open after rotation
      :ok = Manifest.close(rotated)
      {:ok, recovered} = Manifest.open(manifest_name(c), c.tmp_dir)
      assert %{seq: 3} = Manifest.snapshot(recovered, [:seq])
    end
  end

  describe "close/1" do
    test "closes the manifest", c do
      manifest = open_manifest(c)

      assert :ok = Manifest.close(manifest)
    end
  end
end
