defmodule Goblin.MemTableTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Goblin.MemTable

  setup do
    %{mem_table: MemTable.new()}
  end

  describe "new/0, delete/1" do
    test "can create new mem table" do
      assert %MemTable{} = MemTable.new()
    end

    test "can delete created table" do
      assert :ok == MemTable.new() |> MemTable.delete()
    end
  end

  describe "append/2" do
    test "can round-trip", ctx do
      commits = [{:foo, 0, :bar}, {:baz, 1, :baq}]
      assert 1 == MemTable.append(ctx.mem_table, commits)
      assert [{:foo, 0, :bar}] == MemTable.search(ctx.mem_table, [:foo], 2)
      assert [{:baz, 1, :baq}] == MemTable.search(ctx.mem_table, [:baz], 2)
    end

    test "latest append overshadows previous appends", ctx do
      commits = [{:foo, 0, :bar}]
      assert 0 == MemTable.append(ctx.mem_table, commits)
      assert [{:foo, 0, :bar}] == MemTable.search(ctx.mem_table, [:foo], 2)
      commits = [{:foo, 1, :bar2}]
      assert 1 == MemTable.append(ctx.mem_table, commits)
      assert [{:foo, 1, :bar2}] == MemTable.search(ctx.mem_table, [:foo], 2)
    end
  end

  describe "has_key?/2, search/3" do
    test "provides correct membership status", ctx do
      refute MemTable.has_key?(ctx.mem_table, :foo)
      MemTable.append(ctx.mem_table, [{:foo, 0, :bar}])
      assert MemTable.has_key?(ctx.mem_table, :foo)
    end

    test "returns correct value for provided key(s)", ctx do
      assert [] == MemTable.search(ctx.mem_table, [:foo], 2)
      assert [] == MemTable.search(ctx.mem_table, [:bar], 2)
      MemTable.append(ctx.mem_table, [{:foo, 0, :bar}, {:baz, 1, :baq}])
      assert [{:foo, 0, :bar}] == MemTable.search(ctx.mem_table, [:foo], 2)
      assert [{:baz, 1, :baq}] == MemTable.search(ctx.mem_table, [:baz], 2)
    end

    test "returns latest version below provided seq", ctx do
      MemTable.append(ctx.mem_table, [{:foo, 0, :bar}])
      MemTable.append(ctx.mem_table, [{:a, 1, :a}])
      MemTable.append(ctx.mem_table, [{:b, 2, :b}])
      MemTable.append(ctx.mem_table, [{:c, 3, :c}])
      MemTable.append(ctx.mem_table, [{:d, 4, :d}])
      MemTable.append(ctx.mem_table, [{:foo, 5, :bar}])

      assert [{:foo, 0, :bar}] == MemTable.search(ctx.mem_table, [:foo], 5)
    end

    test "returns empty if search sequence < commit sequence", ctx do
      MemTable.append(ctx.mem_table, [{:foo, 1, :bar}])
      assert [] == MemTable.search(ctx.mem_table, [:foo], 0)
      assert [] == MemTable.search(ctx.mem_table, [:foo], 1)
    end
  end

  describe "stream/1/2" do
    test "streams entire MemTable with multiple versions of keys", ctx do
      MemTable.append(ctx.mem_table, [{:foo, 0, :bar}])
      MemTable.append(ctx.mem_table, [{:baz, 1, :baq}])
      MemTable.append(ctx.mem_table, [{:foo, 2, :bar2}])

      assert [
               {:foo, 2, :bar2},
               {:baz, 1, :baq},
               {:foo, 0, :bar}
             ]
             |> Enum.sort_by(&elem(&1, 0)) ==
               MemTable.stream(ctx.mem_table) |> Enum.to_list()
    end

    test "streams MemTable up to provided sequence (exclusive)", ctx do
      MemTable.append(ctx.mem_table, [{:foo, 0, :bar}])
      MemTable.append(ctx.mem_table, [{:baz, 1, :baq}])
      MemTable.append(ctx.mem_table, [{:foo, 2, :bar2}])

      assert [
               {:baz, 1, :baq},
               {:foo, 0, :bar}
             ]
             |> Enum.sort_by(&elem(&1, 0)) ==
               MemTable.stream(ctx.mem_table, 2) |> Enum.to_list()
    end
  end

  describe "size/1" do
    test "increases for each append", ctx do
      size1 = MemTable.size(ctx.mem_table)
      MemTable.append(ctx.mem_table, [{:foo, 0, :bar}])
      assert size1 < MemTable.size(ctx.mem_table)
    end
  end

  @tag :property_tests
  property "always shows latest written value", ctx do
    seq_gen = repeatedly(fn -> System.unique_integer([:positive, :monotonic]) end)

    check all(
            key <- term(),
            vals <- list_of(term(), length: 3)
          ) do
      commits =
        for val <- vals do
          [seq] = Enum.take(seq_gen, 1)
          {key, seq, val}
        end

      {_, seq, val} = List.last(commits)
      assert seq == MemTable.append(ctx.mem_table, commits)
      assert MemTable.has_key?(ctx.mem_table, key)
      assert [{key, seq, val}] == MemTable.search(ctx.mem_table, [key], seq + 1)
    end
  end

  @tag :property_tests
  property "appends are idempotent", ctx do
    check all(
            key <- term(),
            val <- term(),
            seq <- repeatedly(fn -> System.unique_integer([:positive, :monotonic]) end),
            commit = [{key, seq, val}]
          ) do
      MemTable.append(ctx.mem_table, commit)
      assert [{key, seq, val}] == MemTable.search(ctx.mem_table, [key], seq + 1)
      MemTable.append(ctx.mem_table, commit)
      assert [{key, seq, val}] == MemTable.search(ctx.mem_table, [key], seq + 1)
    end
  end

  @tag :property_tests
  property "stream sort order by key is preserved" do
    seq_gen = repeatedly(fn -> System.unique_integer([:positive, :monotonic]) end)

    check all(
            keys <- list_of(term(), length: 5),
            vals <- list_of(term(), length: 5)
          ) do
      mt = MemTable.new()

      commits =
        Enum.zip_with(keys, vals, fn key, val ->
          [seq] = Enum.take(seq_gen, 1)
          {key, seq, val}
        end)

      MemTable.append(mt, commits)
      assert Enum.sort(keys) == MemTable.stream(mt) |> Enum.map(&elem(&1, 0))
    end
  end
end
