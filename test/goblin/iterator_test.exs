defmodule Goblin.IteratorTest do
  use ExUnit.Case, async: true
  alias Goblin.Iterator

  defmodule TestIterator do
    defstruct [
      :data
    ]

    defimpl Goblin.Iterable do
      def init(iterator), do: iterator

      def next(%{data: []}), do: :ok

      def next(iterator) do
        [next | data] = iterator.data
        {next, %{iterator | data: data}}
      end

      def close(_iterator) do
        send(self(), :closed)
        :ok
      end
    end
  end

  describe "k_merge_stream/2" do
    test "merges multiple iterators into stream" do
      {iterators, data} = random_iterators_and_data()

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end) ==
               Iterator.k_merge_stream(iterators) |> Enum.to_list()
    end

    test "merges multiple iterators within provided range" do
      min = 5
      max = 15

      {iterators, data} = random_iterators_and_data()

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end)
             |> Enum.filter(fn {key, _seq, _val} -> key >= min and key <= max end) ==
               Iterator.k_merge_stream(iterators, min: min, max: max) |> Enum.to_list()
    end

    test "filters tombstones" do
      {iterators, data} = random_iterators_and_data(true)

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end)
             |> Enum.reject(fn {_key, _seq, val} -> val == :"$goblin_tombstone" end) ==
               Iterator.k_merge_stream(iterators, filter_tombstones: true)
               |> Enum.to_list()
    end

    test "closes iterators when finished" do
      {iterators, data} = random_iterators_and_data()

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end) ==
               Iterator.k_merge_stream(iterators, filter_tombstones: true) |> Enum.to_list()

      for _ <- 1..length(iterators) do
        assert_receive :closed
      end
    end
  end

  defp random_iterators_and_data(tombstones? \\ false) do
    counter = :counters.new(1, [])

    for _ <- 1..5, reduce: {[], []} do
      {iterators_acc, data_acc} ->
        data =
          1..20
          |> Enum.take_random(Enum.random(1..10))
          |> Enum.map(fn n ->
            seq = :counters.get(counter, 1)
            :counters.add(counter, 1, 1)

            value =
              if tombstones? and :rand.uniform() > 0.5 do
                :"$goblin_tombstone"
              else
                n
              end

            {n, seq, value}
          end)
          |> List.keysort(0)

        {[%TestIterator{data: data} | iterators_acc], data ++ data_acc}
    end
  end
end
