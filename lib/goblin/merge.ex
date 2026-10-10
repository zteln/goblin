defmodule Goblin.Merge do
  @moduledoc false
  import Goblin.Sentinel

  @spec stream((-> list(Enumerable.t())), keyword()) :: Enumerable.t()
  def stream(init, opts \\ []) do
    min = Keyword.get(opts, :min, unset())
    max = Keyword.get(opts, :max, unset())
    filter? = Keyword.get(opts, :filter_tombstones?, true)

    Stream.resource(
      fn -> {build_heap(init.()), unset()} end,
      fn acc -> step(acc, min, max, filter?) end,
      &close_all/1
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

  defp step({heap, last}, min, max, filter?) do
    if :gb_trees.is_empty(heap) do
      {:halt, {heap, last}}
    else
      {{k, _}, {cont, {_, _, v} = triple}, heap} = :gb_trees.take_smallest(heap)
      heap = insert_next(heap, cont)

      cond do
        is_set(max) and k > max -> {:halt, {heap, last}}
        k == last -> {[], {heap, last}}
        is_set(min) and k < min -> {[], {heap, k}}
        filter? and is_tombstone(v) -> {[], {heap, k}}
        true -> {[triple], {heap, k}}
      end
    end
  end

  defp insert_next(heap, cont) do
    case cont.({:cont, nil}) do
      {:suspended, {k, s, _v} = triple, next_cont} ->
        :gb_trees.insert({k, -s}, {next_cont, triple}, heap)

      _ ->
        heap
    end
  end

  defp close_all({heap, _last}) do
    :gb_trees.values(heap)
    |> Enum.each(fn {cont, _} -> cont.({:halt, nil}) end)
  end
end
