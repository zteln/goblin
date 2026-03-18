defmodule Goblin.WALTest do
  use ExUnit.Case, async: true

  alias Goblin.WAL

  @moduletag :tmp_dir

  defp wal_name(c), do: :"#{c.test}"
  defp wal_path(c), do: Path.join(c.tmp_dir, "wal.goblin")

  describe "open/3" do
    test "opens a WAL file successfully", c do
      assert {:ok, wal} = WAL.open(wal_name(c), wal_path(c))

      assert %WAL{} = wal
      assert wal.log_file == wal_path(c)
    end

    test "opens in read-only mode", c do
      path = wal_path(c)
      # create the file first in write mode, then re-open read-only
      {:ok, wal} = WAL.open(wal_name(c), path)
      :ok = WAL.close(wal)

      assert {:ok, _wal} = WAL.open(wal_name(c), path, false)
    end
  end

  describe "append/2" do
    test "appends writes and syncs to disk", c do
      path = wal_path(c)
      {:ok, wal} = WAL.open(wal_name(c), path)
      %{size: size_before} = File.stat!(path)

      assert {:ok, ^wal} = WAL.append(wal, [{:put, 0, :key, :val}])

      %{size: size_after} = File.stat!(path)
      assert size_after > size_before
    end
  end

  describe "replay/1" do
    test "streams back all appended entries", c do
      {:ok, wal} = WAL.open(wal_name(c), wal_path(c))

      writes = [{:put, 0, :key, :val}, {:remove, 1, :key}]
      {:ok, wal} = WAL.append(wal, writes)

      assert writes == WAL.replay(wal) |> Enum.to_list()
    end
  end

  describe "close/1" do
    test "closes the WAL", c do
      {:ok, wal} = WAL.open(wal_name(c), wal_path(c))

      assert :ok = WAL.close(wal)
    end
  end

  describe "rm/1" do
    test "deletes the WAL file", c do
      path = wal_path(c)
      {:ok, wal} = WAL.open(wal_name(c), path)
      :ok = WAL.close(wal)

      assert File.exists?(path)
      assert :ok = WAL.rm(wal)
      refute File.exists?(path)
    end
  end
end
