defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Goblin.Manifest

  @moduletag :tmp_dir

  test "newly created manifest returns empty snapshot", ctx do
    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    assert [_log1, _log2] = Manifest.snapshot(manifest)
  end

  test "snapshots are durable", ctx do
    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    assert {:ok, manifest} = Manifest.update(manifest, ["foo", "bar"], [])
    snapshot = Manifest.snapshot(manifest)
    assert :ok == Manifest.close(manifest)

    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    assert snapshot == Manifest.snapshot(manifest)
  end

  test "recovers previous version when latest is deleted", ctx do
    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    [log1, _log2] = Manifest.snapshot(manifest)
    assert {:ok, manifest} = Manifest.update(manifest, ["foo"], [])
    assert {:ok, manifest} = Manifest.update(manifest, ["bar"], ["foo"])
    assert :ok == Manifest.close(manifest)

    File.rm!(log1)

    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    assert Path.join(ctx.tmp_dir, "foo") in Manifest.snapshot(manifest)
    refute Path.join(ctx.tmp_dir, "bar") in Manifest.snapshot(manifest)
  end

  test "recovers latest version despite trailing garbage", ctx do
    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    [log1, _log2] = Manifest.snapshot(manifest)
    assert {:ok, manifest} = Manifest.update(manifest, ["foo"], [])
    assert {:ok, manifest} = Manifest.update(manifest, ["bar"], ["foo"])
    assert :ok == Manifest.close(manifest)

    File.write!(log1, :binary.copy(<<0xFF>>, 100), [:append])

    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    refute Path.join(ctx.tmp_dir, "foo") in Manifest.snapshot(manifest)
    assert Path.join(ctx.tmp_dir, "bar") in Manifest.snapshot(manifest)
  end

  test "recovers previous version when latest is truncated", ctx do
    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    [log1, _log2] = Manifest.snapshot(manifest)
    assert {:ok, manifest} = Manifest.update(manifest, ["foo"], [])
    assert {:ok, manifest} = Manifest.update(manifest, ["bar"], ["foo"])
    assert :ok == Manifest.close(manifest)

    # corrupt file mid-write
    log_size = :filelib.file_size(log1)
    cutoff = log_size - div(log_size, 2)
    {:ok, f} = :file.open(log1, [:raw, :read, :binary, :write])
    {:ok, _} = :file.position(f, cutoff)
    :ok = :file.truncate(f)
    :ok = :file.close(f)

    assert {:ok, manifest} = Manifest.open(ctx.tmp_dir)
    assert Path.join(ctx.tmp_dir, "foo") in Manifest.snapshot(manifest)
    refute Path.join(ctx.tmp_dir, "bar") in Manifest.snapshot(manifest)
  end

  test "round-trips", ctx do
    {:ok, manifest} = Manifest.open(ctx.tmp_dir)

    assert {:ok, manifest} = Manifest.update(manifest, ["foo"], [])
    snapshot = Manifest.snapshot(manifest)
    assert Enum.any?(snapshot, &String.ends_with?(&1, "foo"))

    assert {:ok, manifest} = Manifest.update(manifest, ["bar"], ["foo"])
    snapshot = Manifest.snapshot(manifest)
    assert Enum.any?(snapshot, &String.ends_with?(&1, "bar"))
    assert Enum.all?(snapshot, &(not String.ends_with?(&1, "foo")))
  end
end
