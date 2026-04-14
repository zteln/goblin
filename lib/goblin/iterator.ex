defmodule Goblin.Iterator do
  @moduledoc false

  @spec linear_stream((-> Goblin.Iterable.t())) :: Enumerable.t(Goblin.triple())
  def linear_stream(init_f) do
    Stream.resource(
      fn ->
        init_f.()
        |> Goblin.Iterable.init()
      end,
      fn iterator ->
        case iterate(iterator) do
          :ok -> {:halt, nil}
          {triple, iterator} -> {[triple], iterator}
        end
      end,
      fn _ -> :ok end
    )
  end

  @spec k_merge_stream((-> [Goblin.Iterable.t()]), keyword()) ::
          Enumerable.t(Goblin.triple())
  def k_merge_stream(init_f, opts \\ []) do
    Stream.resource(
      fn ->
        init_f.()
        |> Enum.reduce(:gb_trees.empty(), fn iterator, acc ->
          iterator = Goblin.Iterable.init(iterator)

          case iterate(iterator) do
            :ok ->
              acc

            {{key, seq, _val} = triple, iterator} ->
              :gb_trees.insert({key, -seq, make_ref()}, {iterator, triple}, acc)
          end
        end)
      end,
      &k_merge(&1, opts),
      fn heap ->
        :gb_trees.values(heap)
        |> Enum.each(fn {iterator, _} ->
          Goblin.Iterable.deinit(iterator)
        end)

        opts[:after] && opts[:after].()
      end
    )
  end

  defp k_merge(heap, opts) do
    filter_tombstones? = Keyword.get(opts, :filter_tombstones?, true)
    min = opts[:min]
    max = opts[:max]

    case take_smallest(heap) do
      :empty ->
        {:halt, heap}

      {{key, _, _}, heap} when not is_nil(max) and key > max ->
        {:halt, heap}

      {{key, _, _}, heap} when not is_nil(min) and key < min ->
        {[], heap}

      {{_, _, :"$goblin_tombstone"}, heap} when filter_tombstones? ->
        {[], heap}

      {triple, heap} ->
        {[triple], heap}
    end
  end

  defp take_smallest(heap) do
    if :gb_trees.is_empty(heap) do
      :empty
    else
      {{key, _, _}, {_iterator, triple}} = :gb_trees.smallest(heap)
      heap = advance_next_key(heap, key)
      {triple, heap}
    end
  end

  defp advance_next_key(heap, key) do
    if :gb_trees.is_empty(heap) do
      heap
    else
      case :gb_trees.smallest(heap) do
        {{k, _, _}, _cursor} when k == key ->
          {_, value, heap} = :gb_trees.take_smallest(heap)
          heap = reinsert_advanced(heap, key, value)
          advance_next_key(heap, key)

        _ ->
          heap
      end
    end
  end

  defp reinsert_advanced(heap, key, cursor) do
    case advance_cursor(cursor, key) do
      nil -> heap
      {_, {k, s, _}} = cursor -> :gb_trees.insert({k, -s, make_ref()}, cursor, heap)
    end
  end

  defp advance_cursor({iterator, {key1, _, _}}, key2) when key1 == key2 do
    case iterate(iterator) do
      :ok ->
        nil

      {{k, _, _} = triple, iterator} when k == key2 ->
        advance_cursor({iterator, triple}, key2)

      {triple, iterator} ->
        {iterator, triple}
    end
  end

  defp iterate(iterator) do
    case Goblin.Iterable.next(iterator) do
      :ok -> Goblin.Iterable.deinit(iterator)
      {out, iterator} -> {out, iterator}
    end
  end
end
