defmodule Goblin.KMergerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Goblin.KMerger

  describe "k_merge/2" do
    test "empty streams results in empty merge" do
      assert [] == KMerger.k_merge(fn -> [[], []] end) |> Enum.to_list()
    end

    test "merges multiple sorted streams" do
      s1 = Stream.map([{:a, 1, "1"}, {:c, 3, "3"}, {:e, 5, "5"}], & &1)
      s2 = Stream.map([{:b, 2, "2"}, {:d, 4, "4"}, {:f, 6, "6"}], & &1)

      result = KMerger.k_merge(fn -> [s1, s2] end) |> Enum.to_list()

      assert result == [
               {:a, 1, "1"},
               {:b, 2, "2"},
               {:c, 3, "3"},
               {:d, 4, "4"},
               {:e, 5, "5"},
               {:f, 6, "6"}
             ]
    end

    test "highest seq wins when key is duplicated across streams" do
      s1 = Stream.map([{:x, 10, "new"}, {:z, 11, "z"}], & &1)
      s2 = Stream.map([{:x, 5, "old"}, {:y, 6, "y"}], & &1)

      result = KMerger.k_merge(fn -> [s1, s2] end) |> Enum.to_list()

      assert result == [{:x, 10, "new"}, {:y, 6, "y"}, {:z, 11, "z"}]
    end

    test "highest seq wins when key is duplicated within stream" do
      s = Stream.map([{:x, 10, "new"}, {:x, 0, "old"}, {:z, 11, "z"}], & &1)

      result = KMerger.k_merge(fn -> [s] end) |> Enum.to_list()

      assert result == [{:x, 10, "new"}, {:z, 11, "z"}]
    end

    test "can filter tombstones" do
      s = Stream.map([{:x, 10, "x"}, {:y, 11, :"$goblin_tombstone"}, {:z, 12, "z"}], & &1)

      without_tombstones =
        KMerger.k_merge(fn -> [s] end, filter_tombstones?: true) |> Enum.to_list()

      with_tombstones =
        KMerger.k_merge(fn -> [s] end, filter_tombstones?: false) |> Enum.to_list()

      assert without_tombstones == [{:x, 10, "x"}, {:z, 12, "z"}]
      assert with_tombstones == [{:x, 10, "x"}, {:y, 11, :"$goblin_tombstone"}, {:z, 12, "z"}]
    end

    test "highest seq tombstone shadows older values" do
      s = Stream.map([{:x, 10, :"$goblin_tombstone"}, {:x, 5, "x"}, {:z, 12, "z"}], & &1)

      assert [{:z, 12, "z"}] =
               KMerger.k_merge(fn -> [s] end, filter_tombstones?: true) |> Enum.to_list()

      assert [{:x, 10, :"$goblin_tombstone"}, {:z, 12, "z"}] =
               KMerger.k_merge(fn -> [s] end, filter_tombstones?: false) |> Enum.to_list()
    end

    test "lower seq tombstone does not shadow newer value" do
      s = Stream.map([{:x, 10, "x"}, {:x, 5, :"$goblin_tombstone"}], & &1)

      assert [{:x, 10, "x"}] =
               KMerger.k_merge(fn -> [s] end, filter_tombstones?: true) |> Enum.to_list()

      assert [{:x, 10, "x"}] =
               KMerger.k_merge(fn -> [s] end, filter_tombstones?: false) |> Enum.to_list()
    end

    test "respects bounds" do
      s = Stream.map([{1, 0, "a"}, {2, 0, "b"}, {3, 0, "c"}, {4, 0, "d"}], & &1)

      assert [{1, 0, "a"}, {2, 0, "b"}] ==
               KMerger.k_merge(fn -> [s] end, max: 2) |> Enum.to_list()

      assert [{3, 0, "c"}, {4, 0, "d"}] ==
               KMerger.k_merge(fn -> [s] end, min: 3) |> Enum.to_list()
    end

    test ":after runs when completed" do
      me = self()
      s1 = Stream.map([{:a, 1, "1"}, {:c, 3, "3"}, {:e, 5, "5"}], & &1)
      s2 = Stream.map([{:b, 2, "2"}, {:d, 4, "4"}, {:f, 6, "6"}], & &1)

      KMerger.k_merge(fn -> [s1, s2] end, after: fn -> send(me, :in_after) end)
      |> Enum.to_list()

      assert_receive :in_after
    end

    test ":after runs when halting early" do
      me = self()
      s1 = Stream.map([{:a, 1, "1"}, {:c, 3, "3"}, {:e, 5, "5"}], & &1)
      s2 = Stream.map([{:b, 2, "2"}, {:d, 4, "4"}, {:f, 6, "6"}], & &1)

      KMerger.k_merge(fn -> [s1, s2] end, after: fn -> send(me, :in_after) end)
      |> Enum.take(1)

      assert_receive :in_after
    end

    test "each source's after_fun runs when completing" do
      me = self()

      s1 =
        Stream.resource(
          fn -> [{:a, 1, "1"}, {:c, 3, "3"}, {:e, 5, "5"}] end,
          fn
            [] -> {:halt, nil}
            [h | t] -> {[h], t}
          end,
          fn _ -> send(me, :in_after1) end
        )

      s2 =
        Stream.resource(
          fn -> [{:b, 2, "2"}, {:d, 4, "4"}, {:f, 6, "6"}] end,
          fn
            [] -> {:halt, nil}
            [h | t] -> {[h], t}
          end,
          fn _ -> send(me, :in_after2) end
        )

      KMerger.k_merge(fn -> [s1, s2] end)
      |> Enum.to_list()

      assert_receive :in_after1
      assert_receive :in_after2
    end

    test "each source's after_fun runs when halting early" do
      me = self()

      s1 =
        Stream.resource(
          fn -> [{:a, 1, "1"}, {:c, 3, "3"}, {:e, 5, "5"}] end,
          fn
            [] -> {:halt, nil}
            [h | t] -> {[h], t}
          end,
          fn _ -> send(me, :in_after1) end
        )

      s2 =
        Stream.resource(
          fn -> [{:b, 2, "2"}, {:d, 4, "4"}, {:f, 6, "6"}] end,
          fn
            [] -> {:halt, nil}
            [h | t] -> {[h], t}
          end,
          fn _ -> send(me, :in_after2) end
        )

      KMerger.k_merge(fn -> [s1, s2] end)
      |> Enum.take(1)

      assert_receive :in_after1
      assert_receive :in_after2
    end
  end

  @tag :property_tests
  property "merged result is sorted by key in ascending order" do
    check all(streams <- list_of(stream_generator(), min_length: 0, max_length: 20)) do
      merged = KMerger.k_merge(fn -> streams end)

      actual =
        streams
        |> Stream.concat()
        |> Enum.sort_by(fn {k, s, _} -> {k, -s} end)
        |> Enum.reduce([], fn
          {k1, _, _}, [{k2, _, _} | _] = acc when k1 == k2 -> acc
          triple, acc -> [triple | acc]
        end)
        |> Enum.reverse()

      assert Enum.to_list(merged) == actual
    end
  end

  defp stream_generator do
    gen all(
          keys <- list_of(term(), min_length: 0, max_length: 20),
          seqs <-
            list_of(repeatedly(fn -> System.unique_integer([:positive, :monotonic]) end),
              min_length: 0,
              max_length: 20
            ),
          vals <- list_of(term(), min_length: 0, max_length: 20)
        ) do
      Enum.zip_with([keys, seqs, vals], fn [key, seq, val] -> {key, seq, val} end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
    end
  end
end
