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
    {:ok, _next_seq, mt} = MemTable.new(path)
    mt
  end

  describe "new/1" do
    test "creates a fresh memtable backed by a WAL file", ctx do
      mt = open_mt(ctx)

      assert mt.id == wal_path(ctx)
      assert File.exists?(wal_path(ctx))
      refute MemTable.has_key?(mt, :anything)
      assert MemTable.stream(mt) |> Enum.to_list() == []
    end

    test "fresh file: next_seq is 0", ctx do
      {:ok, next_seq, _mt} = MemTable.new(wal_path(ctx))

      assert next_seq == 0
    end

    test "reopens with next_seq one past the largest persisted seq", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v0"}, {:b, 1, "v1"}])
      :ok = MemTable.close(mt)

      {:ok, next_seq, reopened} = MemTable.new(wal_path(ctx))

      assert next_seq == 2
      assert MemTable.search(reopened, [:a, :b], 10) == [{:a, 0, "v0"}, {:b, 1, "v1"}]
    end

    test "recovers from trailing garbage appended after a crash", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v0"}, {:b, 1, "v1"}])
      :ok = MemTable.close(mt)

      valid_size = :filelib.file_size(wal_path(ctx))
      File.write!(wal_path(ctx), :binary.copy(<<0xFF>>, 512), [:append])

      {:ok, next_seq, reopened} = MemTable.new(wal_path(ctx))

      assert MemTable.search(reopened, [:a, :b], 10) == [{:a, 0, "v0"}, {:b, 1, "v1"}]
      assert next_seq == 2

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

      {:ok, next_seq, reopened} = MemTable.new(wal_path(ctx))

      assert MemTable.search(reopened, [:a], 10) == [{:a, 0, "v0"}]
      assert MemTable.search(reopened, [:b], 10) == []
      assert next_seq == 1

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

    test "writes persist across close + reopen", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "v"}])
      :ok = MemTable.close(mt)

      {:ok, _next_seq, reopened} = MemTable.new(wal_path(ctx))

      assert MemTable.search(reopened, [:a], 10) == [{:a, 0, "v"}]
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

  describe "rotate/2" do
    test "produces a new memtable backed by a different file with no contents", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:k, 0, "v"}])

      {:ok, new_mt} = MemTable.rotate(mt, filer: new_filer(ctx))

      assert new_mt.id != mt.id
      assert File.exists?(new_mt.id)
      refute MemTable.has_key?(new_mt, :k)
      assert MemTable.search(new_mt, [:k], 10) == []
      assert MemTable.stream(new_mt) |> Enum.to_list() == []
    end

    test "the rotated file remains readable on reopen", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:k, 0, "v"}])
      original_id = mt.id

      {:ok, _new_mt} = MemTable.rotate(mt, filer: new_filer(ctx))

      {:ok, next_seq, reopened} = MemTable.new(original_id)

      assert next_seq == 1
      assert MemTable.search(reopened, [:k], 10) == [{:k, 0, "v"}]
    end

    test "the new memtable accepts independent appends", ctx do
      mt = open_mt(ctx)
      {:ok, mt} = MemTable.append(mt, [{:a, 0, "a"}])

      {:ok, new_mt} = MemTable.rotate(mt, filer: new_filer(ctx))
      {:ok, new_mt} = MemTable.append(new_mt, [{:b, 1, "b"}])

      assert MemTable.search(new_mt, [:a, :b], 10) == [{:b, 1, "b"}]
    end
  end

  describe "destroy/1" do
    test "deletes the in-memory ETS table", ctx do
      mt = open_mt(ctx)
      assert :ets.info(mt.ref) != :undefined

      :ok = MemTable.destroy(mt)

      assert :ets.info(mt.ref) == :undefined
    end

    test "does not touch the WAL file on disk", ctx do
      mt = open_mt(ctx)
      assert File.exists?(wal_path(ctx))

      :ok = MemTable.destroy(mt)

      assert File.exists?(wal_path(ctx))
    end
  end

  describe "close/1" do
    test "closes a fresh memtable cleanly", ctx do
      mt = open_mt(ctx)
      assert :ok = MemTable.close(mt)
    end
  end
end
