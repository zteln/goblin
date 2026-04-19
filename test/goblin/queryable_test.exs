defmodule Goblin.QueryableTest do
  use ExUnit.Case, async: true

  alias Goblin.Mem
  alias Goblin.Queryable

  @moduletag :tmp_dir

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

  describe "Mem implementation" do
    setup ctx do
      {:ok, mem} = Mem.new(Path.join(ctx.tmp_dir, "mem.goblin"), [])

      {:ok, mem} =
        Mem.append_commits(mem, [
          {:put, 0, :a, "v0"},
          {:put, 1, :b, "v1"},
          {:put, 2, :a, "v2"}
        ])

      %{mem: mem}
    end

    test "has_key?/2 delegates to Mem", ctx do
      assert Queryable.has_key?(ctx.mem, :a)
      assert Queryable.has_key?(ctx.mem, :b)
      refute Queryable.has_key?(ctx.mem, :c)
    end

    test "search/3 returns matching triples", ctx do
      result = Queryable.search(ctx.mem, [:a, :b], 3)

      # search returns the latest version below seq for each key
      assert {:a, 2, "v2"} in result
      assert {:b, 1, "v1"} in result
      assert length(result) == 2
    end

    test "stream/4 returns a Mem.Iterator", ctx do
      result = Queryable.stream(ctx.mem, nil, nil, 10)

      assert %Mem.Iterator{} = result
      assert result.max_seq == 10
    end
  end
end
