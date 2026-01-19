defmodule Goblin.IteratorTest do
  use ExUnit.Case, async: true

  defmodule TestIterator do
    defstruct [
      :data
    ]

    defimpl Goblin.Iterable do
      def init(iterator), do: iterator

      def deinit(_iterator) do
        send(self(), :closed)
        :ok
      end

      def next(%{data: []}), do: :ok

      def next(iterator) do
        [next | data] = iterator.data
        {next, %{iterator | data: data}}
      end
    end
  end

  describe "linear_stream/1" do
    test "iterates through all values of a single iterator" do
      {[iterator], [list]} = random_iterators_and_data(num_lists: 1)
      assert list == Goblin.Iterator.linear_stream(iterator) |> Enum.to_list()
    end
  end

  describe "k_merge_stream/2" do
    test "merges multiple iterators into stream" do
      {iterators, lists} = random_iterators_and_data()

      data =
        lists
        |> List.flatten()
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> TestHelper.uniq_by_value(&elem(&1, 0))

      assert data ==
               Goblin.Iterator.k_merge_stream(iterators)
               |> Enum.to_list()
    end

    test "merges multiple iterators within provided range" do
      min = 5
      max = 15

      {iterators, lists} = random_iterators_and_data()

      data =
        lists
        |> List.flatten()
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> TestHelper.uniq_by_value(&elem(&1, 0))
        |> Enum.filter(fn {key, _seq, _val} -> key >= min and key <= max end)

      assert data ==
               Goblin.Iterator.k_merge_stream(iterators, min: min, max: max)
               |> Enum.to_list()
    end

    test "filters tombstones" do
      {iterators, lists} = random_iterators_and_data(tombstone?: true)

      data =
        lists
        |> List.flatten()
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> TestHelper.uniq_by_value(&elem(&1, 0))
        |> Enum.reject(fn {_key, _seq, val} -> val == :"$goblin_tombstone" end)

      assert data ==
               Goblin.Iterator.k_merge_stream(iterators, filter_tombstones: true)
               |> Enum.to_list()
    end

    test "closes iterators when finished" do
      {iterators, lists} = random_iterators_and_data()

      data =
        lists
        |> List.flatten()
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> TestHelper.uniq_by_value(&elem(&1, 0))

      assert data ==
               Goblin.Iterator.k_merge_stream(iterators, filter_tombstones: true)
               |> Enum.to_list()

      for _ <- 1..length(iterators) do
        assert_receive :closed
      end
    end
  end

  defp random_iterators_and_data(opts \\ []) do
    num_lists = opts[:num_lists] || 5
    items_per_list = opts[:items_per_list] || 10
    tombstone? = opts[:tombstone?] || false
    keys = StreamData.term() |> Enum.take(20)

    {lists, _final_seq} =
      1..num_lists
      |> Enum.map_reduce(0, fn _list_idx, seq ->
        {list, new_seq} =
          generate_list(keys, items_per_list, seq, tombstone?)

        {list, new_seq}
      end)

    iterators =
      lists
      |> Enum.map(fn list ->
        %TestIterator{data: list}
      end)

    {iterators, lists}
  end

  defp generate_list(keys, num_items, start_seq, tombstone?) do
    entries =
      1..num_items
      |> Enum.map_reduce(start_seq, fn _i, seq ->
        key = Enum.random(keys)

        val =
          if tombstone? and :rand.uniform() > 0.5 do
            :"$goblin_tombstone"
          else
            [val] = StreamData.term() |> Enum.take(1)
            val
          end

        {{key, seq, val}, seq + 1}
      end)

    {list, final_seq} = entries

    sorted_list = Enum.sort_by(list, fn {key, seq, _val} -> {key, -seq} end)

    {sorted_list, final_seq}
  end
end
