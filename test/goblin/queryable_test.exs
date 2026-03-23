defmodule Goblin.QueryableTest do
  use ExUnit.Case, async: true

  alias Goblin.Queryable
  alias Goblin.MemTable

  describe "List implementation" do
    test "has_key?/2 checks for key presence" do
      list = [{:a, 0, "v1"}, {:b, 1, "v2"}]

      assert Queryable.has_key?(list, :a)
      assert Queryable.has_key?(list, :b)
      refute Queryable.has_key?(list, :c)
    end

    test "search/3 filters by keys and seq" do
      list = [{:a, 0, "v0"}, {:a, 5, "v5"}, {:b, 1, "v1"}, {:c, 2, "v2"}]

      result = Queryable.search(list, [:a, :c], 3)
      assert [{:a, 0, "v0"}, {:c, 2, "v2"}] == result
    end

    test "stream/4 filters by min/max bounds and seq" do
      list = [{1, 0, "a"}, {2, 1, "b"}, {3, 2, "c"}, {4, 3, "d"}, {5, 4, "e"}]

      # No bounds
      assert list == Queryable.stream(list, nil, nil, 100)

      # Min only
      assert [{3, 2, "c"}, {4, 3, "d"}, {5, 4, "e"}] ==
               Queryable.stream(list, 3, nil, 100)

      # Max only
      assert [{1, 0, "a"}, {2, 1, "b"}] ==
               Queryable.stream(list, nil, 2, 100)

      # Both bounds
      assert [{2, 1, "b"}, {3, 2, "c"}, {4, 3, "d"}] ==
               Queryable.stream(list, 2, 4, 100)

      # Seq filtering
      assert [{1, 0, "a"}, {2, 1, "b"}] ==
               Queryable.stream(list, nil, nil, 2)
    end
  end

  describe "MemTable implementation" do
    setup do
      mem_table = MemTable.new()
      MemTable.insert(mem_table, :a, 0, "v0")
      MemTable.insert(mem_table, :b, 1, "v1")
      MemTable.insert(mem_table, :a, 2, "v2")

      %{mem_table: mem_table}
    end

    test "has_key?/2 delegates to MemTable", ctx do
      assert Queryable.has_key?(ctx.mem_table, :a)
      assert Queryable.has_key?(ctx.mem_table, :b)
      refute Queryable.has_key?(ctx.mem_table, :c)
    end

    test "search/3 returns matching triples", ctx do
      result = Queryable.search(ctx.mem_table, [:a, :b], 3)

      # search returns the latest version below seq for each key
      assert {:a, 2, "v2"} in result
      assert {:b, 1, "v1"} in result
      assert length(result) == 2
    end

    test "stream/4 returns a MemTable.Iterator", ctx do
      result = Queryable.stream(ctx.mem_table, nil, nil, 10)

      assert %MemTable.Iterator{} = result
      assert result.max_seq == 10
    end
  end
end
