defmodule Goblin.IteratorTest do
  use ExUnit.Case, async: true
  alias Goblin.Iterator

  describe "stream_k_merge/2" do
    test "merges multiple iterators into stream" do
      assert [
               {1, 6, 6},
               {6, 11, 11},
               {11, 16, 16},
               {16, 21, 21},
               {21, 26, 26}
             ] ==
               Iterator.stream_k_merge(gen_iterator())
               |> Enum.to_list()
    end

    test "merges multiple iterators within range" do
      assert [
               {6, 11, 11},
               {11, 16, 16},
               {16, 21, 21}
             ] ==
               Iterator.stream_k_merge(gen_iterator(), min: 6, max: 16)
               |> Enum.to_list()
    end

    test "filters tombstone values" do
      assert [
               {1, 6, 6},
               {6, 11, 11},
               {11, 16, 16},
               {21, 26, 26}
             ] ==
               Iterator.stream_k_merge(gen_iterator(tombstone_key: 16), filter_tombstones: true)
               |> Enum.to_list()
    end

    test "closes all iterators" do
      close = fn _ ->
        send(self(), :iterator_closed)
        :ok
      end

      assert [
               {1, 6, 6},
               {6, 11, 11},
               {11, 16, 16},
               {16, 21, 21},
               {21, 26, 26}
             ] ==
               Iterator.stream_k_merge(gen_iterator(close: close))
               |> Enum.to_list()

      for _n <- 1..5 do
        assert_receive :iterator_closed
      end
    end
  end

  defp gen_iterator(opts \\ []) do
    tombstone_key = opts[:tombstone_key]
    close = opts[:close] || fn _ -> :ok end

    fn ->
      for n <- 1..25//5 do
        data =
          for i <- n..(n + 5) do
            val = if n == tombstone_key, do: :"$goblin_tombstone", else: i
            {n, i, val}
          end

        next = fn
          [] -> :ok
          [hd | tl] -> {hd, tl}
        end

        {data, next, close}
      end
    end
  end
end
