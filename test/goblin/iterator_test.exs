defmodule Goblin.IteratorTest do
  use ExUnit.Case, async: true

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
        |> Enum.uniq_by(&elem(&1, 0))

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
        |> Enum.uniq_by(&elem(&1, 0))

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end)
             |> Enum.filter(fn {key, _seq, _val} -> key >= min and key <= max end) ==
               Goblin.Iterator.k_merge_stream(iterators, min: min, max: max)
               |> Enum.to_list()
    end

    test "filters tombstones" do
      {iterators, lists} = random_iterators_and_data(tombstone?: true)

      data =
        lists
        |> List.flatten()
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> Enum.uniq_by(&elem(&1, 0))

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end)
             |> Enum.reject(fn {_key, _seq, val} -> val == :"$goblin_tombstone" end) ==
               Goblin.Iterator.k_merge_stream(iterators, filter_tombstones: true)
               |> Enum.to_list()
    end

    test "closes iterators when finished" do
      {iterators, lists} = random_iterators_and_data()

      data =
        lists
        |> List.flatten()
        |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
        |> Enum.uniq_by(&elem(&1, 0))

      assert data
             |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
             |> Enum.uniq_by(fn {key, _seq, _val} -> key end) ==
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
    key_range = opts[:key_range] || 1..20
    versions_per_key = opts[:versions_per_key] || 1..8
    tombstone? = opts[:tombstone?] || false

    {lists, _final_seq} =
      1..num_lists
      |> Enum.map_reduce(0, fn _list_idx, seq ->
        {list, new_seq} =
          generate_list(items_per_list, key_range, versions_per_key, seq, tombstone?)

        {list, new_seq}
      end)

    iterators =
      lists
      |> Enum.map(fn list ->
        %TestIterator{data: list}
      end)

    {iterators, lists}
  end

  defp generate_list(num_items, key_range, versions_per_key, start_seq, tombstone?) do
    keys = Enum.to_list(key_range)

    entries =
      1..num_items
      |> Enum.map_reduce(start_seq, fn _i, seq ->
        key = Enum.random(keys)
        versions = Enum.random(versions_per_key)

        val =
          if tombstone? and :rand.uniform() > 0.5 do
            :"$goblin_tombstone"
          else
            "value_#{key}_v#{versions}_s#{seq}"
          end

        {{key, seq, val}, seq + 1}
      end)

    {list, final_seq} = entries

    sorted_list = Enum.sort_by(list, fn {key, seq, _val} -> {key, -seq} end)

    {sorted_list, final_seq}
  end
end
