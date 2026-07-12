defmodule Goblin.Merge do
  @moduledoc false

  @spec stream((-> list(Enumerable.t())), keyword()) :: Enumerable.t()
  def stream(init, opts \\ []) do
    min = Keyword.get(opts, :min, :"$goblin_nil")
    max = Keyword.get(opts, :max, :"$goblin_nil")
    filter_tombstones? = Keyword.get(opts, :filter_tombstones?, true)

    Stream.resource(
      fn -> init.() |> build_heap() end,
      fn heap -> step(heap, min, max, filter_tombstones?) end,
      fn heap -> close_all(heap, opts) end
    )
  end

  defp build_heap(streams) do
    Enum.reduce(streams, :gb_trees.empty(), fn stream, acc ->
      cont = fn cmd ->
        Enumerable.reduce(stream, cmd, fn e, _ -> {:suspend, e} end)
      end

      insert_next(acc, cont)
    end)
  end

  defp step(heap, min, max, filter_tombstones?) do
    case take_smallest(heap) do
      :empty ->
        {:halt, heap}

      {{key, _, _}, heap} when max != :"$goblin_nil" and key > max ->
        {:halt, heap}

      {{key, _, _}, heap} when min != :"$goblin_nil" and key < min ->
        {[], heap}

      {{_, _, :"$goblin_tombstone"}, heap} when filter_tombstones? ->
        {[], heap}

      {triple, heap} ->
        {[triple], heap}
    end
  end

  defp close_all(heap, opts) do
    :gb_trees.values(heap)
    |> Enum.each(fn {cont, _} -> cont.({:halt, nil}) end)

    opts[:after] && opts[:after].()
  end

  defp insert_next(heap, cont) do
    case advance(cont) do
      {:ok, {k, s, _v} = triple, next_cont} ->
        :gb_trees.insert({k, -s, make_ref()}, {next_cont, triple}, heap)

      :done ->
        heap
    end
  end

  defp advance(cont) do
    case cont.({:cont, nil}) do
      {:suspended, triple, next_cont} -> {:ok, triple, next_cont}
      _ -> :done
    end
  end

  defp take_smallest(heap) do
    if :gb_trees.is_empty(heap) do
      :empty
    else
      {{key, _, _}, {cont, triple}, heap} = :gb_trees.take_smallest(heap)

      heap =
        heap
        |> drain_key(key)
        |> advance_past_key(cont, key)

      {triple, heap}
    end
  end

  defp drain_key(heap, key) do
    if :gb_trees.is_empty(heap) do
      heap
    else
      {{k, _, _}, _} = :gb_trees.smallest(heap)

      if k == key do
        {_, {cont, _}, heap} = :gb_trees.take_smallest(heap)

        heap
        |> advance_past_key(cont, key)
        |> drain_key(key)
      else
        heap
      end
    end
  end

  defp advance_past_key(heap, cont, key) do
    case advance(cont) do
      :done ->
        heap

      {:ok, {k, _, _}, next_cont} when k == key ->
        advance_past_key(heap, next_cont, key)

      {:ok, {k, s, _} = triple, next_cont} ->
        :gb_trees.insert({k, -s, make_ref()}, {next_cont, triple}, heap)
    end
  end
end
