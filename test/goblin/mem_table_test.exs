defmodule Goblin.MemTableTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable

  @moduletag :tmp_dir

  defp wal_path(ctx), do: Path.join(ctx.tmp_dir, "test.wal")

  defp open_mem_table(ctx, opts \\ []) do
    opts = Keyword.merge([sequence: :infinity, write?: true], opts)
    {:ok, mem_table} = MemTable.open(:"#{ctx.test}", wal_path(ctx), opts)
    mem_table
  end

  describe "open/3" do
    test "opens and returns a MemTable struct", ctx do
      mem_table = open_mem_table(ctx)

      assert %MemTable{wal: wal, store: store} = mem_table
      assert wal != nil
      assert store != nil
    end

    test "replays WAL on re-open", ctx do
      mem_table = open_mem_table(ctx)
      :ok = MemTable.append_commits(mem_table, [{:put, 0, :a, "v1"}, {:put, 1, :b, "v2"}])
      :ok = MemTable.close(mem_table)

      mem_table = open_mem_table(ctx)

      results = Goblin.Queryable.search(mem_table, [:a, :b], 2)
      assert {:a, 0, "v1"} in results
      assert {:b, 1, "v2"} in results
    end
  end

  describe "append_commits/2" do
    test "appends put commits and makes data queryable", ctx do
      mem_table = open_mem_table(ctx)

      :ok = MemTable.append_commits(mem_table, [{:put, 0, :key, "value"}])

      assert [{:key, 0, "value"}] = Goblin.Queryable.search(mem_table, [:key], 1)
    end

    test "appends remove commits as tombstones", ctx do
      mem_table = open_mem_table(ctx)

      :ok = MemTable.append_commits(mem_table, [{:put, 0, :key, "value"}])
      :ok = MemTable.append_commits(mem_table, [{:remove, 1, :key}])

      assert [{:key, 1, :"$goblin_tombstone"}] =
               Goblin.Queryable.search(mem_table, [:key], 2)
    end
  end

  describe "rotate?/2" do
    test "returns false when store is small", ctx do
      mem_table = open_mem_table(ctx)

      refute MemTable.rotate?(mem_table, 1024)
    end

    test "returns true when store exceeds limit", ctx do
      mem_table = open_mem_table(ctx)

      commits =
        Enum.map(0..100, fn i ->
          {:put, i, :"key_#{i}", String.duplicate("x", 100)}
        end)

      :ok = MemTable.append_commits(mem_table, commits)

      assert MemTable.rotate?(mem_table, 1)
    end
  end

  describe "wal_path/1" do
    test "returns the WAL file path", ctx do
      mem_table = open_mem_table(ctx)

      assert MemTable.wal_path(mem_table) == wal_path(ctx)
    end
  end

  describe "close/1" do
    test "closes the mem_table", ctx do
      mem_table = open_mem_table(ctx)

      assert :ok = MemTable.close(mem_table)
    end
  end

  describe "remove_wal/1" do
    test "removes the WAL file from disk", ctx do
      mem_table = open_mem_table(ctx)
      :ok = MemTable.close(mem_table)

      assert File.exists?(wal_path(ctx))
      assert :ok = MemTable.remove_wal(mem_table)
      refute File.exists?(wal_path(ctx))
    end
  end

  describe "delete_table/1" do
    test "deletes the underlying ETS table", ctx do
      mem_table = open_mem_table(ctx)
      table_id = mem_table.store.ref

      assert :ets.info(table_id) != :undefined

      :ok = MemTable.delete_table(mem_table)

      assert :ets.info(table_id) == :undefined
    end
  end
end
