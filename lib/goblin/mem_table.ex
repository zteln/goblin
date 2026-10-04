defmodule Goblin.MemTable do
  @moduledoc false

  defstruct [
    :ref,
    level_key: -1
  ]

  @type t :: %__MODULE__{ref: :ets.tid()}

  @spec new() :: t()
  def new(), do: %__MODULE__{ref: :ets.new(:mem_table, [:ordered_set])}

  @spec delete(t()) :: :ok
  def delete(mt) do
    :ets.delete(mt.ref)
    :ok
  end

  @spec append(t(), list({term(), non_neg_integer(), term()})) :: non_neg_integer()
  def append(mt, commits) do
    Enum.reduce(commits, -1, fn {key, sqn, val}, acc ->
      :ets.insert(mt.ref, {{key, -sqn}, val})
      max(acc, sqn)
    end)
  end

  @spec search(t(), list(term()), non_neg_integer()) :: list({term(), non_neg_integer(), term()})
  def search(mt, keys, sqn) do
    Enum.flat_map(keys, fn key ->
      case search_table(mt, key, sqn) do
        nil -> []
        triple -> [triple]
      end
    end)
  end

  @spec stream(t()) :: Enumerable.t({term(), non_neg_integer(), term()})
  @spec stream(t(), non_neg_integer() | :infinity) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  @spec stream(t(), term(), non_neg_integer() | :infinity) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def stream(mt), do: do_stream(mt, fn -> iterate(mt) end, :infinity)
  def stream(mt, max_sqn), do: do_stream(mt, fn -> iterate(mt) end, max_sqn)

  def stream(mt, min_key, max_sqn),
    do: do_stream(mt, fn -> iterate(mt, {min_key, max_sqn}) end, max_sqn)

  defp do_stream(mt, start, max_sqn) do
    Stream.resource(
      start,
      fn
        :end_of_iteration ->
          {:halt, nil}

        {key, sqn} = idx when sqn < max_sqn ->
          case get(mt, key, sqn) do
            nil -> {[], iterate(mt, idx)}
            triple -> {[triple], iterate(mt, idx)}
          end

        idx ->
          {[], iterate(mt, idx)}
      end,
      fn _ -> :ok end
    )
  end

  defp get(mt, key, sqn) do
    case :ets.lookup(mt.ref, {key, -sqn}) do
      [] -> nil
      [{_, value}] -> {key, sqn, value}
    end
  end

  defp search_table(mt, key, sqn) do
    case :ets.next(mt.ref, {key, -sqn}) do
      {k, s} when key == k ->
        [{_, val}] = :ets.lookup(mt.ref, {k, s})
        {key, abs(s), val}

      _ ->
        nil
    end
  end

  defp iterate(mt) do
    idx = :ets.first(mt.ref)
    handle_iteration(mt, idx)
  end

  defp iterate(mt, {key, sqn}) do
    idx = :ets.next(mt.ref, {key, -sqn})
    handle_iteration(mt, idx)
  end

  defp handle_iteration(_mt, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_mt, {key, sqn}), do: {key, abs(sqn)}
end
