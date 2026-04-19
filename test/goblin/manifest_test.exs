defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true

  alias Goblin.Manifest

  @moduletag :tmp_dir

  defp open_manifest(ctx) do
    {:ok, manifest} = Manifest.open(ctx.tmp_dir)
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
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          42
        )

      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert %{sequence: 42} = Manifest.snapshot(recovered, [:sequence])
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
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          0
        )

      assert %{disk_tables: [dt]} = Manifest.snapshot(manifest, [:disk_tables])
      assert String.ends_with?(dt, "sst_0.goblin")
    end
  end

  describe "snapshot/2" do
    test "returns only requested keys with resolved paths", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          10
        )

      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal2.goblin"))

      seq_only = Manifest.snapshot(manifest, [:sequence])
      assert seq_only == %{sequence: 10}
      refute Map.has_key?(seq_only, :wal)

      wal_only = Manifest.snapshot(manifest, [:wal])
      assert %{wal: wal} = wal_only
      assert wal == Path.join(ctx.tmp_dir, "wal2.goblin")
      refute Map.has_key?(wal_only, :sequence)
    end
  end

  describe "current_files/1" do
    test "returns manifest log files", ctx do
      manifest = open_manifest(ctx)

      files = Manifest.current_files(manifest)
      assert length(files) > 0
      assert Enum.all?(files, &String.contains?(&1, "manifest.goblin"))
    end

    test "excludes unrelated files", ctx do
      File.write!(Path.join(ctx.tmp_dir, "unrelated.txt"), "hello")
      manifest = open_manifest(ctx)

      files = Manifest.current_files(manifest)
      refute Enum.any?(files, &String.contains?(&1, "unrelated.txt"))
    end
  end

  describe "add_compaction/3" do
    test "adds new disk tables and marks old ones for removal", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          0
        )

      {:ok, manifest} =
        Manifest.add_compaction(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_1.goblin")],
          [Path.join(ctx.tmp_dir, "sst_0.goblin")]
        )

      snapshot = Manifest.snapshot(manifest, [:disk_tables, :dirt])

      assert Enum.any?(snapshot.disk_tables, &String.ends_with?(&1, "sst_1.goblin"))
      refute Enum.any?(snapshot.disk_tables, &String.ends_with?(&1, "sst_0.goblin"))
      assert Enum.any?(snapshot.dirt, &String.ends_with?(&1, "sst_0.goblin"))
    end
  end

  describe "clear_dirt/1" do
    test "clears dirt after compaction", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          0
        )

      {:ok, manifest} =
        Manifest.add_compaction(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_1.goblin")],
          [Path.join(ctx.tmp_dir, "sst_0.goblin")]
        )

      manifest = Manifest.clear_dirt(manifest)
      assert %{dirt: []} = Manifest.snapshot(manifest, [:dirt])
    end

    test "preserves other snapshot state", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          10
        )

      {:ok, manifest} =
        Manifest.add_compaction(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_1.goblin")],
          [Path.join(ctx.tmp_dir, "sst_0.goblin")]
        )

      manifest = Manifest.clear_dirt(manifest)
      snapshot = Manifest.snapshot(manifest, [:sequence, :disk_tables])

      assert %{sequence: 10} = snapshot
      assert Enum.any?(snapshot.disk_tables, &String.ends_with?(&1, "sst_1.goblin"))
    end
  end

  describe "recovery" do
    test "recovers full state after lifecycle with compaction", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          5
        )

      {:ok, manifest} =
        Manifest.add_compaction(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_1.goblin")],
          [Path.join(ctx.tmp_dir, "sst_0.goblin")]
        )

      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)
      snapshot = Manifest.snapshot(recovered, [:sequence, :disk_tables])

      assert %{sequence: 5} = snapshot
      assert Enum.any?(snapshot.disk_tables, &String.ends_with?(&1, "sst_1.goblin"))
      refute Enum.any?(snapshot.disk_tables, &String.ends_with?(&1, "sst_0.goblin"))
    end

    test "supports further updates after recovery", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          5
        )

      :ok = Manifest.close(manifest)

      {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal2.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_1.goblin")],
          Path.join(ctx.tmp_dir, "wal2.goblin"),
          10
        )

      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)
      assert %{sequence: 10} = Manifest.snapshot(recovered, [:sequence])
    end
  end

  describe "close/1" do
    test "closes the manifest", c do
      manifest = open_manifest(c)

      assert :ok = Manifest.close(manifest)
    end
  end

  describe "crash recovery" do
    defp manifest_path(ctx), do: Path.join(ctx.tmp_dir, "manifest.goblin")
    defp manifest_tmp_path(ctx), do: manifest_path(ctx) <> ".tmp"

    test "recovers from trailing garbage appended to the log after a crash", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          7
        )

      :ok = Manifest.close(manifest)

      valid_size = :filelib.file_size(manifest_path(ctx))
      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(manifest_path(ctx), garbage, [:append])

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      snapshot = Manifest.snapshot(recovered, [:sequence, :disk_tables])
      assert %{sequence: 7} = snapshot
      assert Enum.any?(snapshot.disk_tables, &String.ends_with?(&1, "sst_0.goblin"))

      :ok = Manifest.close(recovered)
      # The garbage tail must be truncated by FileIO.stream!(truncate?: true).
      assert :filelib.file_size(manifest_path(ctx)) == valid_size
    end

    test "recovers from a log truncated mid-block by a crash", ctx do
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          3
        )

      # Add an extra update, then chop the file so that its trailing block
      # is cut off mid-header — simulating a crash mid-flush.
      {:ok, manifest} =
        Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal2.goblin"))

      :ok = Manifest.close(manifest)

      full_size = :filelib.file_size(manifest_path(ctx))
      {:ok, f} = :file.open(manifest_path(ctx), [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(f, full_size - 512 + 8)
      :ok = :file.truncate(f)
      :file.close(f)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      # The add_wal that was chopped off is lost; the prior flush survives.
      snapshot = Manifest.snapshot(recovered, [:sequence, :wal])
      assert %{sequence: 3} = snapshot
      # The surviving wal is the one written before the truncated update.
      assert snapshot.wal == nil or
               String.ends_with?(snapshot.wal, "wal.goblin")

      :ok = Manifest.close(recovered)
      assert :filelib.file_size(manifest_path(ctx)) == full_size - 512
    end

    test "recovers from a rotation interrupted after the rename", ctx do
      # Simulate: rotation renamed manifest.goblin → manifest.goblin.tmp and
      # crashed before a new manifest.goblin could be created. On reopen,
      # Manifest.open/1 must rename the .tmp back and recover state from it.
      manifest = open_manifest(ctx)
      {:ok, manifest} = Manifest.add_wal(manifest, Path.join(ctx.tmp_dir, "wal.goblin"))

      {:ok, manifest} =
        Manifest.add_flush(
          manifest,
          [Path.join(ctx.tmp_dir, "sst_0.goblin")],
          Path.join(ctx.tmp_dir, "wal.goblin"),
          9
        )

      :ok = Manifest.close(manifest)

      :ok = File.rename(manifest_path(ctx), manifest_tmp_path(ctx))
      refute File.exists?(manifest_path(ctx))
      assert File.exists?(manifest_tmp_path(ctx))

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert File.exists?(manifest_path(ctx))
      refute File.exists?(manifest_tmp_path(ctx))

      assert %{sequence: 9} = Manifest.snapshot(recovered, [:sequence])

      :ok = Manifest.close(recovered)
    end
  end
end
