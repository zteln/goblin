defmodule Goblin.MemTest do
  use ExUnit.Case, async: true

  alias Goblin.Brokerable
  alias Goblin.Mem
  alias Goblin.Queryable

  @moduletag :tmp_dir

  defp mem_path(ctx), do: Path.join(ctx.tmp_dir, "mem.goblin")

  defp open_mem(ctx, opts \\ []) do
    {:ok, mem} = Mem.new(mem_path(ctx), opts)
    mem
  end

  describe "new/2" do
    test "opens and returns a Mem struct", ctx do
      mem = open_mem(ctx)

      assert %Mem{io: io, path: path, table: table, max_sequence: -1} = mem
      assert path == mem_path(ctx)
      assert io != nil
      assert table != nil
    end

    test "replays persisted commits on re-open", ctx do
      mem = open_mem(ctx)

      {:ok, mem} =
        Mem.append_commits(mem, [{:put, 0, :a, "v1"}, {:put, 1, :b, "v2"}])

      :ok = Mem.close(mem)

      mem = open_mem(ctx)

      results = Queryable.search(mem, [:a, :b], 2)
      assert {:a, 0, "v1"} in results
      assert {:b, 1, "v2"} in results
      assert Mem.sequence(mem) == 2
    end

    test "recovers from a WAL with trailing garbage appended after a crash", ctx do
      mem = open_mem(ctx)
      {:ok, mem} = Mem.append_commits(mem, [{:put, 0, :a, "v1"}, {:put, 1, :b, "v2"}])
      :ok = Mem.close(mem)

      valid_size = :filelib.file_size(mem_path(ctx))
      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(mem_path(ctx), garbage, [:append])

      mem = open_mem(ctx)

      results = Queryable.search(mem, [:a, :b], 2)
      assert {:a, 0, "v1"} in results
      assert {:b, 1, "v2"} in results
      assert Mem.sequence(mem) == 2

      :ok = Mem.close(mem)
      assert :filelib.file_size(mem_path(ctx)) == valid_size
    end

    test "recovers from a WAL truncated mid-block by a crash", ctx do
      mem = open_mem(ctx)
      {:ok, mem} = Mem.append_commits(mem, [{:put, 0, :a, "v1"}])
      {:ok, mem} = Mem.append_commits(mem, [{:put, 1, :b, "v2"}])
      :ok = Mem.close(mem)

      # Simulate a partial final flush: chop the last block off at a
      # header-sized offset so reopening sees an invalid trailing header.
      full_size = :filelib.file_size(mem_path(ctx))
      {:ok, f} = :file.open(mem_path(ctx), [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(f, full_size - 512 + 8)
      :ok = :file.truncate(f)
      :file.close(f)

      mem = open_mem(ctx)

      assert [{:a, 0, "v1"}] = Queryable.search(mem, [:a], 1)
      assert [] = Queryable.search(mem, [:b], 2)
      assert Mem.sequence(mem) == 1

      :ok = Mem.close(mem)
      # The truncated tail must be gone after recovery.
      assert :filelib.file_size(mem_path(ctx)) == full_size - 512
    end
  end

  describe "append_commits/2" do
    test "appends put commits and makes data queryable", ctx do
      mem = open_mem(ctx)

      {:ok, mem} = Mem.append_commits(mem, [{:put, 0, :key, "value"}])

      assert Queryable.has_key?(mem, :key)
      assert [{:key, 0, "value"}] = Queryable.search(mem, [:key], 1)
    end

    test "appends remove commits as tombstones", ctx do
      mem = open_mem(ctx)

      {:ok, mem} = Mem.append_commits(mem, [{:put, 0, :key, "value"}])
      {:ok, mem} = Mem.append_commits(mem, [{:remove, 1, :key}])

      assert [{:key, 0, "value"}] = Queryable.search(mem, [:key], 1)
      assert [{:key, 1, :"$goblin_tombstone"}] = Queryable.search(mem, [:key], 2)
    end

    test "advances sequence/1 to the last commit's seq + 1", ctx do
      mem = open_mem(ctx)

      {:ok, mem} = Mem.append_commits(mem, [{:put, 5, :a, "v"}])
      assert Mem.sequence(mem) == 6

      {:ok, mem} = Mem.append_commits(mem, [{:put, 9, :b, "v"}])
      assert Mem.sequence(mem) == 10
    end
  end

  describe "rotate?/2" do
    test "returns false when table is small", ctx do
      mem = open_mem(ctx)

      refute Mem.rotate?(mem, 1024)
    end

    test "returns true when table exceeds the limit", ctx do
      mem = open_mem(ctx)

      commits =
        Enum.map(0..100, fn i ->
          {:put, i, :"key_#{i}", String.duplicate("x", 100)}
        end)

      {:ok, mem} = Mem.append_commits(mem, commits)

      assert Mem.rotate?(mem, 1)
    end
  end

  describe "disk_path/1" do
    test "returns the path passed to new/2", ctx do
      mem = open_mem(ctx)

      assert Mem.disk_path(mem) == mem_path(ctx)
    end
  end

  describe "sequence/1" do
    test "starts at 0 for a fresh Mem", ctx do
      mem = open_mem(ctx)

      assert Mem.sequence(mem) == 0
    end
  end

  describe "close/1" do
    test "closes the Mem", ctx do
      mem = open_mem(ctx)

      assert :ok = Mem.close(mem)
    end
  end

  describe "remove_disk/1" do
    test "removes the WAL file from disk", ctx do
      mem = open_mem(ctx)
      :ok = Mem.close(mem)

      assert File.exists?(mem_path(ctx))
      assert :ok = Mem.remove_disk(mem)
      refute File.exists?(mem_path(ctx))
    end
  end

  describe "Brokerable" do
    test "id/1 returns the disk path", ctx do
      mem = open_mem(ctx)

      assert Brokerable.id(mem) == mem_path(ctx)
    end

    test "level_key/1 returns -1", ctx do
      mem = open_mem(ctx)

      assert Brokerable.level_key(mem) == -1
    end

    test "remove/1 deletes the underlying ETS table", ctx do
      mem = open_mem(ctx)
      table_id = mem.table.ref

      assert :ets.info(table_id) != :undefined

      :ok = Brokerable.remove(mem)

      assert :ets.info(table_id) == :undefined
    end
  end
end
