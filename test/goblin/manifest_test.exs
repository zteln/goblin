defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true

  alias Goblin.Manifest

  @moduletag :tmp_dir

  defp manifest_name(ctx), do: :"#{ctx.test}"

  defp open_manifest(ctx) do
    {:ok, manifest} = Manifest.open(manifest_name(ctx), ctx.tmp_dir)
    manifest
  end

  describe "open/2" do
    test "opens a fresh manifest with empty snapshot", ctx do
      manifest = open_manifest(ctx)

      assert %{sequence: 0} = Manifest.snapshot(manifest, [:sequence])
      assert %{wal: nil} = Manifest.snapshot(manifest, [:wal])
      assert %{disk_tables: []} = Manifest.snapshot(manifest, [:disk_tables])
    end

    test "recovers state from an existing manifest log", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.update_sequence(manifest, 42)
      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(manifest_name(ctx), ctx.tmp_dir)

      assert %{sequence: 42} = Manifest.snapshot(recovered, [:sequence])
    end
  end

  describe "update_sequence/2" do
    test "persists sequence and updates the snapshot", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} = Manifest.update_sequence(manifest, 5)

      assert %{sequence: 5} = Manifest.snapshot(manifest, [:sequence])
    end
  end

  describe "add_wal/2" do
    test "persists wal path and updates the snapshot", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      assert %{wal: wal} = Manifest.snapshot(manifest, [:wal])
      assert String.ends_with?(wal, "wal.goblin")
    end
  end

  describe "add_flush/3" do
    test "adds disk tables and removes wal", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(manifest, [Path.join(ctx.tmp_dir, "sst_0.goblin")], Path.join(ctx.tmp_dir, "wal.goblin"))

      assert %{disk_tables: [dt]} = Manifest.snapshot(manifest, [:disk_tables])
      assert String.ends_with?(dt, "sst_0.goblin")
    end
  end

  describe "snapshot/2" do
    test "returns only requested keys with resolved paths", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.update_sequence(manifest, 10)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      seq_only = Manifest.snapshot(manifest, [:sequence])
      assert seq_only == %{sequence: 10}
      refute Map.has_key?(seq_only, :wal)

      wal_only = Manifest.snapshot(manifest, [:wal])
      assert %{wal: wal} = wal_only
      assert wal == Path.join(ctx.tmp_dir, "wal.goblin")
      refute Map.has_key?(wal_only, :sequence)
    end
  end

  describe "close/1" do
    test "closes the manifest", c do
      manifest = open_manifest(c)

      assert :ok = Manifest.close(manifest)
    end
  end
end
