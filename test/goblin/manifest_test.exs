defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true

  alias Goblin.Manifest

  @moduletag :tmp_dir

  defp open_manifest(ctx) do
    {:ok, m} = Manifest.open(ctx.tmp_dir)
    m
  end

  defp data_path(ctx, name), do: Path.join(ctx.tmp_dir, name)
  defp manifest_path(ctx), do: data_path(ctx, "manifest.goblin")
  defp manifest_tmp_path(ctx), do: manifest_path(ctx) <> ".tmp"

  describe "open/1" do
    test "returns an empty snapshot for a fresh directory", ctx do
      manifest = open_manifest(ctx)

      assert Manifest.snapshot(manifest) == {0, 0, []}
      assert File.exists?(manifest_path(ctx))
    end

    test "recovers state from an existing log", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(manifest, [{:mem, data_path(ctx, "wal.goblin")}], [], 0)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [{:mem, data_path(ctx, "wal.goblin")}],
          7
        )

      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert Manifest.snapshot(recovered) ==
               {7, 2, [{:disk, data_path(ctx, "sst_0.goblin")}]}
    end

    test "renames manifest.goblin.tmp back when only the tmp file remains", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(manifest, [{:disk, data_path(ctx, "sst_0.goblin")}], [], 9)

      :ok = Manifest.close(manifest)

      :ok = File.rename(manifest_path(ctx), manifest_tmp_path(ctx))
      refute File.exists?(manifest_path(ctx))
      assert File.exists?(manifest_tmp_path(ctx))

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert File.exists?(manifest_path(ctx))
      refute File.exists?(manifest_tmp_path(ctx))
      assert Manifest.snapshot(recovered) ==
               {9, 1, [{:disk, data_path(ctx, "sst_0.goblin")}]}
    end
  end

  describe "update/4" do
    test "adds tagged files and returns full paths under data_dir", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [
            {:mem, data_path(ctx, "wal.goblin")},
            {:disk, data_path(ctx, "sst_0.goblin")}
          ],
          [],
          0
        )

      {_, _, files} = Manifest.snapshot(manifest)

      assert {:mem, data_path(ctx, "wal.goblin")} in files
      assert {:disk, data_path(ctx, "sst_0.goblin")} in files
    end

    test "removes files passed in del", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:mem, data_path(ctx, "wal.goblin")}, {:disk, data_path(ctx, "sst_0.goblin")}],
          [],
          0
        )

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [],
          [{:mem, data_path(ctx, "wal.goblin")}],
          0
        )

      {_, _, files} = Manifest.snapshot(manifest)

      refute {:mem, data_path(ctx, "wal.goblin")} in files
      assert {:disk, data_path(ctx, "sst_0.goblin")} in files
    end

    test "ignores a del that does not match any current file", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [{:disk, data_path(ctx, "never_existed.goblin")}],
          0
        )

      {_, _, files} = Manifest.snapshot(manifest)
      assert files == [{:disk, data_path(ctx, "sst_0.goblin")}]
    end

    test "places newly added files before previously existing ones", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(manifest, [{:disk, data_path(ctx, "a.goblin")}], [], 0)

      {:ok, manifest} =
        Manifest.update(manifest, [{:disk, data_path(ctx, "b.goblin")}], [], 0)

      {_, _, files} = Manifest.snapshot(manifest)

      assert files == [
               {:disk, data_path(ctx, "b.goblin")},
               {:disk, data_path(ctx, "a.goblin")}
             ]
    end

    test "advances the snapshot sequence to the value passed", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} = Manifest.update(manifest, [], [], 100)

      assert {100, 0, []} = Manifest.snapshot(manifest)
    end

    test "no_files counter is cumulative across updates", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(manifest, [{:disk, data_path(ctx, "a.goblin")}], [], 0)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "b.goblin")}],
          [{:disk, data_path(ctx, "a.goblin")}],
          0
        )

      {_, no_files, files} = Manifest.snapshot(manifest)

      assert no_files == 2
      assert files == [{:disk, data_path(ctx, "b.goblin")}]
    end

    test "writes survive close + reopen", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:mem, data_path(ctx, "wal.goblin")}],
          [],
          0
        )

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [{:mem, data_path(ctx, "wal.goblin")}],
          11
        )

      :ok = Manifest.close(manifest)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)
      assert Manifest.snapshot(recovered) == Manifest.snapshot(manifest)
    end
  end

  describe "snapshot/1" do
    test "returns {seq, no_files, files} with absolute paths rooted at data_dir", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [],
          3
        )

      assert {3, 1, [{:disk, path}]} = Manifest.snapshot(manifest)
      assert Path.dirname(path) == ctx.tmp_dir
      assert Path.basename(path) == "sst_0.goblin"
    end
  end

  describe "current_file/1" do
    test "returns the manifest log path under data_dir", ctx do
      manifest = open_manifest(ctx)

      assert Manifest.current_file(manifest) == manifest_path(ctx)
    end
  end

  describe "close/1" do
    test "closes a fresh manifest cleanly", ctx do
      manifest = open_manifest(ctx)

      assert :ok = Manifest.close(manifest)
    end
  end

  describe "crash recovery" do
    test "trailing garbage in the log is truncated on reopen", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [],
          7
        )

      :ok = Manifest.close(manifest)

      valid_size = :filelib.file_size(manifest_path(ctx))
      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(manifest_path(ctx), garbage, [:append])

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert Manifest.snapshot(recovered) ==
               {7, 1, [{:disk, data_path(ctx, "sst_0.goblin")}]}

      :ok = Manifest.close(recovered)
      assert :filelib.file_size(manifest_path(ctx)) == valid_size
    end

    test "a log truncated mid-block discards the partial trailing entry", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [],
          3
        )

      survived_size = :filelib.file_size(manifest_path(ctx))

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:mem, data_path(ctx, "wal2.goblin")}],
          [],
          4
        )

      :ok = Manifest.close(manifest)

      full_size = :filelib.file_size(manifest_path(ctx))
      {:ok, f} = :file.open(manifest_path(ctx), [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(f, full_size - 512 + 8)
      :ok = :file.truncate(f)
      :file.close(f)

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert Manifest.snapshot(recovered) ==
               {3, 1, [{:disk, data_path(ctx, "sst_0.goblin")}]}

      :ok = Manifest.close(recovered)
      assert :filelib.file_size(manifest_path(ctx)) == survived_size
    end

    test "rotation interrupted after the rename is healed on reopen", ctx do
      manifest = open_manifest(ctx)

      {:ok, manifest} =
        Manifest.update(
          manifest,
          [{:disk, data_path(ctx, "sst_0.goblin")}],
          [],
          9
        )

      :ok = Manifest.close(manifest)

      :ok = File.rename(manifest_path(ctx), manifest_tmp_path(ctx))
      refute File.exists?(manifest_path(ctx))
      assert File.exists?(manifest_tmp_path(ctx))

      {:ok, recovered} = Manifest.open(ctx.tmp_dir)

      assert File.exists?(manifest_path(ctx))
      refute File.exists?(manifest_tmp_path(ctx))
      assert Manifest.snapshot(recovered) ==
               {9, 1, [{:disk, data_path(ctx, "sst_0.goblin")}]}

      :ok = Manifest.close(recovered)
    end
  end
end
