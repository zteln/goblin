defmodule Goblin.MemTest do
  use ExUnit.Case, async: true

  alias Goblin.Mem

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
        Mem.append_commits(mem, [{:a, 0, "v1"}, {:b, 1, "v2"}])

      :ok = Mem.close(mem)

      mem = open_mem(ctx)

      results = Mem.search(mem, [:a, :b], 2)
      assert {:a, 0, "v1"} in results
      assert {:b, 1, "v2"} in results
      assert Mem.sequence(mem) == 2
    end

    test "recovers from a WAL with trailing garbage appended after a crash", ctx do
      mem = open_mem(ctx)
      {:ok, mem} = Mem.append_commits(mem, [{:a, 0, "v1"}, {:b, 1, "v2"}])
      :ok = Mem.close(mem)

      valid_size = :filelib.file_size(mem_path(ctx))
      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(mem_path(ctx), garbage, [:append])

      mem = open_mem(ctx)

      results = Mem.search(mem, [:a, :b], 2)
      assert {:a, 0, "v1"} in results
      assert {:b, 1, "v2"} in results
      assert Mem.sequence(mem) == 2

      :ok = Mem.close(mem)
      assert :filelib.file_size(mem_path(ctx)) == valid_size
    end

    test "recovers from a WAL truncated mid-block by a crash", ctx do
      mem = open_mem(ctx)
      {:ok, mem} = Mem.append_commits(mem, [{:a, 0, "v1"}])
      {:ok, mem} = Mem.append_commits(mem, [{:b, 1, "v2"}])
      :ok = Mem.close(mem)

      # Simulate a partial final flush: chop the last block off at a
      # header-sized offset so reopening sees an invalid trailing header.
      full_size = :filelib.file_size(mem_path(ctx))
      {:ok, f} = :file.open(mem_path(ctx), [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(f, full_size - 512 + 8)
      :ok = :file.truncate(f)
      :file.close(f)

      mem = open_mem(ctx)

      assert [{:a, 0, "v1"}] = Mem.search(mem, [:a], 1)
      assert [] = Mem.search(mem, [:b], 2)
      assert Mem.sequence(mem) == 1

      :ok = Mem.close(mem)
      # The truncated tail must be gone after recovery.
      assert :filelib.file_size(mem_path(ctx)) == full_size - 512
    end
  end

  describe "append_commits/2" do
    test "appends put commits and makes data queryable", ctx do
      mem = open_mem(ctx)

      {:ok, mem} = Mem.append_commits(mem, [{:key, 0, "value"}])

      assert Mem.has_key?(mem, :key)
      assert [{:key, 0, "value"}] = Mem.search(mem, [:key], 1)
    end

    test "appends remove commits as tombstones", ctx do
      mem = open_mem(ctx)

      {:ok, mem} = Mem.append_commits(mem, [{:key, 0, "value"}])
      {:ok, mem} = Mem.append_commits(mem, [{:key, 1, :"$goblin_tombstone"}])

      assert [{:key, 0, "value"}] = Mem.search(mem, [:key], 1)
      assert [{:key, 1, :"$goblin_tombstone"}] = Mem.search(mem, [:key], 2)
    end

    test "advances sequence/1 to the last commit's seq + 1", ctx do
      mem = open_mem(ctx)

      {:ok, mem} = Mem.append_commits(mem, [{:a, 5, "v"}])
      assert Mem.sequence(mem) == 6

      {:ok, mem} = Mem.append_commits(mem, [{:b, 9, "v"}])
      assert Mem.sequence(mem) == 10
    end
  end

  describe "stream/2" do
    test "yields all entries in ascending key order", ctx do
      mem = open_mem(ctx)

      {:ok, mem} =
        Mem.append_commits(mem, [
          {:b, 0, "b0"},
          {:a, 1, "a1"},
          {:c, 2, "c2"}
        ])

      result = Mem.stream(mem, 100) |> Enum.to_list()

      assert result == [
               {:a, 1, "a1"},
               {:b, 0, "b0"},
               {:c, 2, "c2"}
             ]
    end

    test "max_seq is exclusive", ctx do
      mem = open_mem(ctx)

      {:ok, mem} =
        Mem.append_commits(mem, [
          {:a, 0, "v0"},
          {:b, 1, "v1"},
          {:c, 2, "v2"}
        ])

      result = Mem.stream(mem, 2) |> Enum.to_list()
      assert result == [{:a, 0, "v0"}, {:b, 1, "v1"}]
    end

    test "yields all versions of a key in descending seq order (dedup is k_merge's job)", ctx do
      mem = open_mem(ctx)

      {:ok, mem} =
        Mem.append_commits(mem, [
          {:k, 0, "old"},
          {:k, 5, "new"}
        ])

      # Mem.stream emits raw triples; Iterator.k_merge dedups across sources.
      result = Mem.stream(mem, 10) |> Enum.to_list()
      assert result == [{:k, 5, "new"}, {:k, 0, "old"}]
    end

    test "empty table yields empty stream", ctx do
      mem = open_mem(ctx)

      assert [] == Mem.stream(mem, 100) |> Enum.to_list()
    end

    test "supports :infinity as max_seq", ctx do
      mem = open_mem(ctx)
      {:ok, mem} = Mem.append_commits(mem, [{:a, 0, "v0"}])

      assert [{:a, 0, "v0"}] == Mem.stream(mem, :infinity) |> Enum.to_list()
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
          {:"key_#{i}", i, String.duplicate("x", 100)}
        end)

      {:ok, mem} = Mem.append_commits(mem, commits)

      assert Mem.rotate?(mem, 1)
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

  describe "delete/1" do
    test "deletes the underlying ETS table", ctx do
      mem = open_mem(ctx)
      table_ref = mem.table.ref

      assert :ets.info(table_ref) != :undefined

      :ok = Mem.delete(mem)

      assert :ets.info(table_ref) == :undefined
    end
  end
end
