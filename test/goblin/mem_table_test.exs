defmodule Goblin.MemTableTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable

  @moduletag :tmp_dir

  defp wal_path(ctx, name \\ "wal.goblin"), do: Path.join(ctx.tmp_dir, name)

  defp new_filer(ctx) do
    counter = :counters.new(1, [])

    fn ->
      n = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      Path.join(ctx.tmp_dir, "rotated-#{n}.goblin")
    end
  end

  defp open_mt(ctx, opts \\ []) do
    path = Keyword.get(opts, :path, wal_path(ctx))
    mem_limit = Keyword.get(opts, :mem_limit, 10 * 1024 * 1024)
    filer = Keyword.get_lazy(opts, :filer, fn -> new_filer(ctx) end)
    {:ok, mt} = MemTable.new(path, mem_limit: mem_limit, filer: filer)
    mt
  end

  describe "new/2" do
    test "creates a fresh memtable with max_sequence 0 and an open WAL file", ctx do
      mt = open_mt(ctx)

      assert mt.id == wal_path(ctx)
      assert mt.max_sequence == 0
      assert File.exists?(wal_path(ctx))
      refute MemTable.has_key?(mt, :anything)
      assert MemTable.stream(mt) |> Enum.to_list() == []
    end

    test "replays persisted commits on reopen", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v0"}, {:b, 1, "v1"}])
      :ok = MemTable.close(mt)

      reopened = open_mt(ctx)

      assert reopened.max_sequence == 2
      assert MemTable.search(reopened, [:a, :b], 10) == [{:a, 0, "v0"}, {:b, 1, "v1"}]
    end

    test "recovers from trailing garbage appended after a crash", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v0"}, {:b, 1, "v1"}])
      :ok = MemTable.close(mt)

      valid_size = :filelib.file_size(wal_path(ctx))
      File.write!(wal_path(ctx), :binary.copy(<<0xFF>>, 512), [:append])

      reopened = open_mt(ctx)

      assert MemTable.search(reopened, [:a, :b], 10) == [{:a, 0, "v0"}, {:b, 1, "v1"}]
      assert reopened.max_sequence == 2

      :ok = MemTable.close(reopened)
      assert :filelib.file_size(wal_path(ctx)) == valid_size
    end

    test "recovers from a WAL truncated mid-block by a crash", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v0"}])
      survived_size = :filelib.file_size(wal_path(ctx))
      {:ok, mt} = MemTable.append(mt, [{:b, 1, "v1"}])
      :ok = MemTable.close(mt)

      full_size = :filelib.file_size(wal_path(ctx))
      {:ok, f} = :file.open(wal_path(ctx), [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(f, full_size - 512 + 8)
      :ok = :file.truncate(f)
      :file.close(f)

      reopened = open_mt(ctx)

      assert MemTable.search(reopened, [:a], 10) == [{:a, 0, "v0"}]
      assert MemTable.search(reopened, [:b], 10) == []
      assert reopened.max_sequence == 1

      :ok = MemTable.close(reopened)
      assert :filelib.file_size(wal_path(ctx)) == survived_size
    end
  end

  describe "append/2" do
    test "puts become queryable via has_key?/search", ctx do
      mt = open_mt(ctx)

      {:ok, mt} = MemTable.append(mt, [{:k, 0, "v"}])

      assert MemTable.has_key?(mt, :k)
      assert MemTable.search(mt, [:k], 10) == [{:k, 0, "v"}]
    end

    test "tombstones are stored alongside earlier values", ctx do
      mt = open_mt(ctx)

      {:ok, mt} = MemTable.append(mt, [{:k, 0, "v"}])
      {:ok, mt} = MemTable.append(mt, [{:k, 1, :"$goblin_tombstone"}])

      assert MemTable.search(mt, [:k], 1) == [{:k, 0, "v"}]
      assert MemTable.search(mt, [:k], 10) == [{:k, 1, :"$goblin_tombstone"}]
    end

    test "max_sequence advances to the next available seq after each append", ctx do
      mt = open_mt(ctx)

      {:ok, mt} = MemTable.append(mt, [{:a, 5, "v"}])
      assert mt.max_sequence == 6

      {:ok, mt} = MemTable.append(mt, [{:b, 9, "v"}])
      assert mt.max_sequence == 10
    end

    test "rotates when ets memory exceeds mem_limit", ctx do
      filer = new_filer(ctx)
      mt = open_mt(ctx, mem_limit: 0, filer: filer)

      assert {:ok, old_mt, new_mt} = MemTable.append(mt, [{:k, 0, "v"}])

      assert old_mt.id == wal_path(ctx)
      assert new_mt.id != old_mt.id
      assert File.exists?(old_mt.id)
      assert File.exists?(new_mt.id)

      assert MemTable.has_key?(old_mt, :k)
      assert MemTable.search(old_mt, [:k], 10) == [{:k, 0, "v"}]

      refute MemTable.has_key?(new_mt, :k)
      assert MemTable.search(new_mt, [:k], 10) == []
      assert new_mt.max_sequence == old_mt.max_sequence
    end
  end

  describe "has_key?/2" do
    test "returns true for any inserted key regardless of seq", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:k, 7, "v"}])

      assert MemTable.has_key?(mt, :k)
    end

    test "returns false for an uninserted key", ctx do
      mt = open_mt(ctx)
      refute MemTable.has_key?(mt, :missing)
    end
  end

  describe "search/3" do
    test "returns the latest version below the seq for each key", ctx do
      mt = open_mt(ctx)

      {:ok, mt} =
        MemTable.append(mt, [
          {:a, 0, "a0"},
          {:a, 5, "a5"},
          {:b, 2, "b2"}
        ])

      assert MemTable.search(mt, [:a, :b], 10) == [{:a, 5, "a5"}, {:b, 2, "b2"}]
      assert MemTable.search(mt, [:a], 5) == [{:a, 0, "a0"}]
    end

    test "skips missing keys", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v"}])

      assert MemTable.search(mt, [:a, :missing], 10) == [{:a, 0, "v"}]
    end

    test "empty memtable yields an empty result", ctx do
      mt = open_mt(ctx)
      assert MemTable.search(mt, [:a, :b], 10) == []
    end
  end

  describe "stream/2" do
    test "yields entries in ascending key order", ctx do
      mt = open_mt(ctx)

      {:ok, mt} =
        MemTable.append(mt, [
          {:b, 0, "b0"},
          {:a, 1, "a1"},
          {:c, 2, "c2"}
        ])

      assert MemTable.stream(mt) |> Enum.to_list() == [
               {:a, 1, "a1"},
               {:b, 0, "b0"},
               {:c, 2, "c2"}
             ]
    end

    test "yields multiple versions of a key in descending seq order", ctx do
      mt = open_mt(ctx)

      {:ok, mt} = MemTable.append(mt, [{:k, 0, "old"}, {:k, 5, "new"}])

      assert MemTable.stream(mt) |> Enum.to_list() == [
               {:k, 5, "new"},
               {:k, 0, "old"}
             ]
    end

    test "filters strictly by max_seq (exclusive)", ctx do
      mt = open_mt(ctx)

      {:ok, mt} =
        MemTable.append(mt, [
          {:a, 0, "v0"},
          {:b, 1, "v1"},
          {:c, 2, "v2"}
        ])

      assert MemTable.stream(mt, 2) |> Enum.to_list() == [
               {:a, 0, "v0"},
               {:b, 1, "v1"}
             ]
    end

    test "defaults max_seq to :infinity", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v0"}])

      assert MemTable.stream(mt) |> Enum.to_list() == [{:a, 0, "v0"}]
    end

    test "empty memtable yields an empty stream", ctx do
      mt = open_mt(ctx)
      assert MemTable.stream(mt) |> Enum.to_list() == []
    end
  end

  describe "close/1" do
    test "closes a fresh memtable cleanly", ctx do
      mt = open_mt(ctx)
      assert :ok = MemTable.close(mt)
    end
  end
end
