defmodule Goblin.MemTable.WALTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable.WAL

  @moduletag :tmp_dir

  defp wal_path(ctx), do: Path.join(ctx.tmp_dir, "test.wal")

  defp open_wal(ctx) do
    {:ok, wal} = WAL.open(wal_path(ctx))
    wal
  end

  describe "open/3" do
    test "opens a new WAL", ctx do
      assert {:ok, %WAL{}} = WAL.open(wal_path(ctx))
    end
  end

  describe "append/2 and replay/1" do
    test "appends commits and replays them in order", ctx do
      wal = open_wal(ctx)

      commits = [{:put, 0, :a, "v1"}, {:put, 1, :b, "v2"}]
      assert :ok = WAL.append(wal, commits)

      replayed = WAL.replay(wal) |> Enum.to_list()
      assert replayed == commits
    end

    test "replays multiple appends in order", ctx do
      wal = open_wal(ctx)

      :ok = WAL.append(wal, [{:put, 0, :a, "v1"}])
      :ok = WAL.append(wal, [{:remove, 1, :a}])

      replayed = WAL.replay(wal) |> Enum.to_list()
      assert replayed == [{:put, 0, :a, "v1"}, {:remove, 1, :a}]
    end
  end

  describe "close/1" do
    test "closes the WAL", ctx do
      wal = open_wal(ctx)

      assert :ok = WAL.close(wal)
    end
  end

  describe "rm/1" do
    test "removes the WAL file from disk", ctx do
      wal = open_wal(ctx)
      :ok = WAL.close(wal)

      assert File.exists?(wal_path(ctx))
      assert :ok = WAL.rm(wal)
      refute File.exists?(wal_path(ctx))
    end
  end

  describe "filepath/1" do
    test "returns the WAL file path", ctx do
      wal = open_wal(ctx)

      assert WAL.filepath(wal) == wal_path(ctx)
    end
  end
end
