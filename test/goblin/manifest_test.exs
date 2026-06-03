defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Goblin.Manifest

  @moduletag :tmp_dir

  describe "open/1, close/1, current_file/1" do
    test "creates new file with empty snapshot for a fresh directory", ctx do
      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      assert {0, [], []} == Manifest.snapshot(manifest)
      assert File.exists?(Manifest.current_file(manifest))
    end

    test "snapshots are durable", ctx do
      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      {:ok, manifest} = Manifest.update(manifest, [{:mem, "foo"}], [{:disk, "bar"}], 2)
      snapshot = Manifest.snapshot(manifest)
      assert :ok == Manifest.close(manifest)

      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      assert snapshot == Manifest.snapshot(manifest)
    end

    test "recovers existing .tmp version of log during startup", ctx do
      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      {:ok, manifest} = Manifest.update(manifest, [{:mem, "foo"}], [{:disk, "bar"}], 2)
      snapshot = Manifest.snapshot(manifest)
      logfile = Manifest.current_file(manifest)
      assert :ok == Manifest.close(manifest)

      File.rename(logfile, logfile <> ".tmp")

      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      assert snapshot == Manifest.snapshot(manifest)
      refute File.exists?(logfile <> ".tmp")
    end

    test "trailing garbage is truncated on reopen", ctx do
      fake_file = Path.join(ctx.tmp_dir, "foo")
      File.touch(fake_file)

      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      {:ok, manifest} = Manifest.update(manifest, [{:mem, fake_file}], [{:disk, "bar"}], 2)
      snapshot = Manifest.snapshot(manifest)
      logfile = Manifest.current_file(manifest)
      assert :ok == Manifest.close(manifest)

      File.write!(logfile, :binary.copy(<<0xFF>>, 100), [:append])
      size = :filelib.file_size(logfile)
      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      assert snapshot == Manifest.snapshot(manifest)
      assert size > :filelib.file_size(logfile)
    end

    test "log is truncated during partial write", ctx do
      fake_file1 = Path.join(ctx.tmp_dir, "foo1")
      File.touch(fake_file1)

      fake_file2 = Path.join(ctx.tmp_dir, "foo2")
      File.touch(fake_file2)

      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      logfile = Manifest.current_file(manifest)
      {:ok, manifest} = Manifest.update(manifest, [{:disk, fake_file1}], [], 2)
      snapshot = Manifest.snapshot(manifest)
      survived_size = :filelib.file_size(logfile)
      {:ok, manifest} = Manifest.update(manifest, [{:disk, fake_file2}], [], 3)
      assert :ok == Manifest.close(manifest)

      # corrupt file mid-write
      {:ok, f} = :file.open(logfile, [:raw, :read, :binary, :write])
      {:ok, _} = :file.position(f, survived_size + 50)
      :ok = :file.truncate(f)
      :ok = :file.close(f)

      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      assert snapshot == Manifest.snapshot(manifest)
      assert survived_size == :filelib.file_size(logfile)
    end

    test "stops if corrupted log returns irredeemable snapshot", ctx do
      fake_file1 = Path.join(ctx.tmp_dir, "foo1")

      fake_file2 = Path.join(ctx.tmp_dir, "foo2")
      File.touch(fake_file2)

      assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
      logfile = Manifest.current_file(manifest)
      {:ok, manifest} = Manifest.update(manifest, [{:disk, fake_file1}], [], 2)
      survived_size = :filelib.file_size(logfile)
      {:ok, manifest} = Manifest.update(manifest, [{:disk, fake_file2}], [], 3)
      assert :ok == Manifest.close(manifest)

      # corrupt file mid-write
      {:ok, f} = :file.open(logfile, [:raw, :read, :binary, :write])
      {:ok, _} = :file.position(f, survived_size + 50)
      :ok = :file.truncate(f)
      :ok = :file.close(f)

      assert {:error, :corrupt_manifest} = Manifest.open(ctx.tmp_dir)
    end
  end

  describe "update/4, snapshot/1" do
    test "round-trips", ctx do
      fake_file1 = {:mem, Path.join(ctx.tmp_dir, "foo1")}
      fake_file2 = {:disk, Path.join(ctx.tmp_dir, "foo2")}
      {:ok, manifest} = Manifest.open(ctx.tmp_dir)

      assert {:ok, manifest} =
               Manifest.update(manifest, [fake_file1, fake_file2], [], 0)

      assert {0, [fake_file1, fake_file2], []} == Manifest.snapshot(manifest)

      assert {:ok, manifest} =
               Manifest.update(manifest, [], [fake_file2], 4)

      {_, fake_file2} = fake_file2

      assert {4, [fake_file1], [fake_file2]} == Manifest.snapshot(manifest)
    end

    test "rotates log file when exceeding max size", ctx do
      {:ok, manifest} = Manifest.open(ctx.tmp_dir, max_log_size: 3 * 512)
      logfile = Manifest.current_file(manifest)
      assert {:ok, manifest} = Manifest.update(manifest, [{:mem, "foo"}], [], 1)
      assert {:ok, manifest} = Manifest.update(manifest, [], [], 2)
      pre_rotation_size = :filelib.file_size(logfile)

      assert {:ok, manifest} = Manifest.update(manifest, [], [{:disk, "bar"}], 3)
      post_rotation_size = :filelib.file_size(logfile)

      assert pre_rotation_size > post_rotation_size

      assert {
               3,
               [{:mem, Path.join(ctx.tmp_dir, "foo")}],
               [Path.join(ctx.tmp_dir, "bar")]
             } == Manifest.snapshot(manifest)
    end
  end

  describe "sweep_dirt/2" do
    test "partially cleans dirt", ctx do
      fake_file1 = {:mem, Path.join(ctx.tmp_dir, "foo1")}
      fake_file2 = {:disk, Path.join(ctx.tmp_dir, "foo2")}
      {:ok, manifest} = Manifest.open(ctx.tmp_dir)

      assert {:ok, manifest} =
               Manifest.update(manifest, [], [fake_file1, fake_file2], 0)

      {_, fake_file1} = fake_file1
      {_, fake_file2} = fake_file2
      assert {0, [], [fake_file1, fake_file2]} == Manifest.snapshot(manifest)
      assert {:ok, manifest} = Manifest.sweep_dirt(manifest, [fake_file1])
      assert {0, [], [fake_file2]} == Manifest.snapshot(manifest)
    end

    test "cleans dirt with provided paths", ctx do
      fake_file1 = {:mem, Path.join(ctx.tmp_dir, "foo1")}
      fake_file2 = {:disk, Path.join(ctx.tmp_dir, "foo2")}
      {:ok, manifest} = Manifest.open(ctx.tmp_dir)

      assert {:ok, manifest} =
               Manifest.update(manifest, [], [fake_file1, fake_file2], 0)

      {_, fake_file1} = fake_file1
      {_, fake_file2} = fake_file2
      assert {0, [], [fake_file1, fake_file2]} == Manifest.snapshot(manifest)
      assert {:ok, manifest} = Manifest.sweep_dirt(manifest, [fake_file1, fake_file2])
      assert {0, [], []} == Manifest.snapshot(manifest)
    end
  end
end
