defmodule Goblin.DiskTable.DiskIndex do
  @moduledoc false

  @spec new() :: list()
  def new(), do: []

  @spec append(list(), term(), non_neg_integer(), non_neg_integer()) :: list()
  def append(index, key, seq, offset) do
    [{key, seq, offset} | index]
  end

  @spec finalize(list()) :: tuple()
  def finalize(index) do
    index = index |> Enum.reverse() |> List.to_tuple()
    {start, _, _} = elem(index, 0)
    {start, index}
  end

  @spec lookup(tuple(), ({term(), non_neg_integer(), non_neg_integer()} -> boolean())) :: {term(), non_neg_integer(), non_neg_integer()} | nil
  def lookup(index, pred) do
    size = tuple_size(index)

    case partition_point(index, pred, 0, size) do
      point when point < size -> elem(index, point)
      _ -> nil
    end
  end

  defp partition_point(_index, _pred, lo, lo), do: lo

  defp partition_point(index, pred, lo, hi) do
    mid = div(lo + hi, 2)

    if pred.(elem(index, mid)),
      do: partition_point(index, pred, mid + 1, hi),
      else: partition_point(index, pred, lo, mid)
  end
end
