defmodule Goblin.WALTest do
  use ExUnit.Case, async: true

  alias Goblin.WAL

  @moduletag :tmp_dir

  defp wal_name(ctx), do: :"#{ctx.test}"
  defp wal_path(ctx), do: Path.join(ctx.tmp_dir, "wal.goblin")

  describe "open/3" do
    test "opens a WAL file successfully", ctx do
      assert {:ok, wal} = WAL.open(wal_name(ctx), wal_path(ctx))

      assert %WAL{} = wal
      assert wal.log_file == wal_path(ctx)
    end

    test "opens in read-only mode", ctx do
      path = wal_path(ctx)
      # create the file first in write mode, then re-open read-only
      {:ok, wal} = WAL.open(wal_name(ctx), path)
      :ok = WAL.close(wal)

      assert {:ok, _wal} = WAL.open(wal_name(ctx), path, false)
    end
  end

  describe "append/2" do
    test "appends writes and syncs to disk", ctx do
      path = wal_path(ctx)
      {:ok, wal} = WAL.open(wal_name(ctx), path)
      %{size: size_before} = File.stat!(path)

      assert {:ok, ^wal} = WAL.append(wal, [{:put, 0, :key, :val}])

      %{size: size_after} = File.stat!(path)
      assert size_after > size_before
    end
  end

  describe "replay/1" do
    test "streams back all appended entries", ctx do
      {:ok, wal} = WAL.open(wal_name(ctx), wal_path(ctx))

      writes = [{:put, 0, :key, :val}, {:remove, 1, :key}]
      {:ok, wal} = WAL.append(wal, writes)

      assert writes == WAL.replay(wal) |> Enum.to_list()
    end
  end

  describe "close/1" do
    test "closes the WAL", ctx do
      {:ok, wal} = WAL.open(wal_name(ctx), wal_path(ctx))

      assert :ok = WAL.close(wal)
    end
  end

  describe "rm/1" do
    test "deletes the WAL file", ctx do
      path = wal_path(ctx)
      {:ok, wal} = WAL.open(wal_name(ctx), path)
      :ok = WAL.close(wal)

      assert File.exists?(path)
      assert :ok = WAL.rm(wal)
      refute File.exists?(path)
    end
  end
end
