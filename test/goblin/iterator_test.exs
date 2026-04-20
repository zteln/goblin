defmodule Goblin.IteratorTest do
  use ExUnit.Case, async: true

  alias Goblin.Iterator

  describe "k_merge/2 — basic merging" do
    test "merges multiple sorted streams in ascending key order" do
      s1 = Stream.map([{:a, 1, "1"}, {:c, 3, "3"}, {:e, 5, "5"}], & &1)
      s2 = Stream.map([{:b, 2, "2"}, {:d, 4, "4"}, {:f, 6, "6"}], & &1)

      result = Iterator.k_merge(fn -> [s1, s2] end) |> Enum.to_list()

      assert result == [
               {:a, 1, "1"},
               {:b, 2, "2"},
               {:c, 3, "3"},
               {:d, 4, "4"},
               {:e, 5, "5"},
               {:f, 6, "6"}
             ]
    end

    test "empty streams produce an empty result" do
      result =
        Iterator.k_merge(fn -> [Stream.map([], & &1), Stream.map([], & &1)] end)
        |> Enum.to_list()

      assert result == []
    end

    test "handles a single source" do
      s = Stream.map([{1, 0, "a"}, {2, 0, "b"}], & &1)
      assert [{1, 0, "a"}, {2, 0, "b"}] == Iterator.k_merge(fn -> [s] end) |> Enum.to_list()
    end
  end

  describe "k_merge/2 — key deduplication" do
    test "cross-source: newer seq wins when same key appears in multiple streams" do
      s_new = Stream.map([{:x, 10, "new"}, {:z, 11, "z"}], & &1)
      s_old = Stream.map([{:x, 5, "old"}, {:y, 6, "y"}], & &1)

      result = Iterator.k_merge(fn -> [s_new, s_old] end) |> Enum.to_list()

      assert result == [{:x, 10, "new"}, {:y, 6, "y"}, {:z, 11, "z"}]
    end

    test "intra-source: single stream with multiple versions of same key emits only newest" do
      # A Mem or Disk source can yield multiple versions of a key (ordered by
      # {key, -seq}). k_merge must dedup within a source too.
      s = Stream.map([{:a, 1, "a"}, {:c, 10, "c_new"}, {:c, 5, "c_old"}, {:e, 7, "e"}], & &1)

      result = Iterator.k_merge(fn -> [s] end) |> Enum.to_list()

      assert result == [{:a, 1, "a"}, {:c, 10, "c_new"}, {:e, 7, "e"}]
    end

    test "same key in multiple sources AND in same source dedups to highest seq" do
      mem = Stream.map([{:c, 20, "mem_new"}, {:c, 15, "mem_old"}], & &1)
      l0 = Stream.map([{:c, 10, "l0"}], & &1)
      l1 = Stream.map([{:c, 1, "l1"}], & &1)

      result = Iterator.k_merge(fn -> [mem, l0, l1] end) |> Enum.to_list()

      assert result == [{:c, 20, "mem_new"}]
    end

    test "three versions of same key in one stream collapse to one" do
      s = Stream.map([{:x, 30, "v30"}, {:x, 20, "v20"}, {:x, 10, "v10"}, {:y, 1, "y"}], & &1)

      result = Iterator.k_merge(fn -> [s] end) |> Enum.to_list()

      assert result == [{:x, 30, "v30"}, {:y, 1, "y"}]
    end
  end

  describe "k_merge/2 — filtering" do
    test "filter_tombstones?: true drops tombstone values" do
      s_mem =
        Stream.map([{:a, 5, :"$goblin_tombstone"}, {:b, 6, "b"}], & &1)

      s_disk = Stream.map([{:a, 1, "a_old"}, {:c, 2, "c"}], & &1)

      result =
        Iterator.k_merge(fn -> [s_mem, s_disk] end, filter_tombstones?: true)
        |> Enum.to_list()

      assert result == [{:b, 6, "b"}, {:c, 2, "c"}]
    end

    test "filter_tombstones?: false passes tombstones through" do
      s = Stream.map([{:a, 5, :"$goblin_tombstone"}, {:b, 6, "b"}], & &1)

      result =
        Iterator.k_merge(fn -> [s] end, filter_tombstones?: false)
        |> Enum.to_list()

      assert result == [{:a, 5, :"$goblin_tombstone"}, {:b, 6, "b"}]
    end

    test "intra-source tombstone masks older value even across streams" do
      # Newer tombstone in mem should suppress the older value in l0.
      s_mem = Stream.map([{:k, 10, :"$goblin_tombstone"}], & &1)
      s_disk = Stream.map([{:k, 5, "old"}], & &1)

      result =
        Iterator.k_merge(fn -> [s_mem, s_disk] end, filter_tombstones?: true)
        |> Enum.to_list()

      assert result == []
    end

    test ":max bound halts emission" do
      s = Stream.map([{1, 0, "a"}, {2, 0, "b"}, {3, 0, "c"}, {4, 0, "d"}], & &1)

      result = Iterator.k_merge(fn -> [s] end, max: 2) |> Enum.to_list()
      assert result == [{1, 0, "a"}, {2, 0, "b"}]
    end

    test ":min bound skips smaller keys" do
      s = Stream.map([{1, 0, "a"}, {2, 0, "b"}, {3, 0, "c"}], & &1)

      result = Iterator.k_merge(fn -> [s] end, min: 2) |> Enum.to_list()
      assert result == [{2, 0, "b"}, {3, 0, "c"}]
    end
  end

  describe "k_merge/2 — resource lifecycle" do
    test "each source's after_fun fires on full consumption" do
      {:ok, agent} = Agent.start_link(fn -> [] end)
      s1 = tracked_stream(agent, :t1, [{:a, 1, "a"}, {:c, 3, "c"}])
      s2 = tracked_stream(agent, :t2, [{:b, 2, "b"}, {:d, 4, "d"}])

      _ = Iterator.k_merge(fn -> [s1, s2] end) |> Enum.to_list()

      events = Agent.get(agent, &Enum.reverse(&1))
      assert Enum.count(events, fn {_, e} -> e == :start end) == 2
      assert Enum.count(events, fn {_, e} -> e == :after end) == 2
    end

    test "each source's after_fun fires on early halt" do
      {:ok, agent} = Agent.start_link(fn -> [] end)
      s1 = tracked_stream(agent, :t1, [{:a, 1, "a"}, {:c, 3, "c"}, {:e, 5, "e"}])
      s2 = tracked_stream(agent, :t2, [{:b, 2, "b"}, {:d, 4, "d"}, {:f, 6, "f"}])

      _ = Iterator.k_merge(fn -> [s1, s2] end) |> Enum.take(2)

      events = Agent.get(agent, &Enum.reverse(&1))
      assert Enum.count(events, fn {_, e} -> e == :start end) == 2
      assert Enum.count(events, fn {_, e} -> e == :after end) == 2
    end

    test "outer :after runs on completion" do
      {:ok, ref} = Agent.start_link(fn -> false end)
      s = Stream.map([{:a, 1, "a"}], & &1)

      _ =
        Iterator.k_merge(fn -> [s] end, after: fn -> Agent.update(ref, fn _ -> true end) end)
        |> Enum.to_list()

      assert Agent.get(ref, & &1)
    end

    test "outer :after runs on early halt" do
      {:ok, ref} = Agent.start_link(fn -> false end)
      s = Stream.map([{:a, 1, "a"}, {:b, 2, "b"}, {:c, 3, "c"}], & &1)

      _ =
        Iterator.k_merge(fn -> [s] end, after: fn -> Agent.update(ref, fn _ -> true end) end)
        |> Enum.take(1)

      assert Agent.get(ref, & &1)
    end
  end

  defp tracked_stream(agent, label, data) do
    Stream.resource(
      fn ->
        Agent.update(agent, &[{label, :start} | &1])
        data
      end,
      fn
        [] -> {:halt, nil}
        [h | t] -> {[h], t}
      end,
      fn _ -> Agent.update(agent, &[{label, :after} | &1]) end
    )
  end
end
