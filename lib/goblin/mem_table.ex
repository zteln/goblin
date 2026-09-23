defmodule Goblin.MemTable do
  @moduledoc false

  defstruct [:id, :tid]

  @type t :: %__MODULE__{}

  @spec new(Path.t()) :: t()
  def new(id), do: %__MODULE__{id: id, tid: :ets.new(:mem_table, [:ordered_set])}

  @spec delete(t()) :: :ok
  def delete(mt), do: :ets.delete(mt.tid)

  @spec append(t(), list({term(), non_neg_integer(), term()})) :: non_neg_integer()
  def append(mt, commits) do
    Enum.reduce(commits, -1, fn {key, seq, val}, acc ->
      :ets.insert(mt.tid, {{key, -seq}, val})
      max(acc, seq)
    end)
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(mt, key) do
    case :ets.prev(mt.tid, {key, 1}) do
      {k, _} when k == key -> true
      _ -> false
    end
  end

  @spec search(t(), list(term()), non_neg_integer()) :: list({term(), non_neg_integer(), term()})
  def search(mt, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case search_table(mt.tid, key, seq) do
        nil -> []
        triple -> [triple]
      end
    end)
  end

  @spec stream(t(), non_neg_integer() | :infinity) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def stream(mt, max_seq \\ :infinity) do
    Stream.resource(
      fn -> iterate(mt) end,
      fn
        :end_of_iteration ->
          {:halt, nil}

        {key, seq} = idx when seq < max_seq ->
          case get(mt, key, seq) do
            nil -> {[], iterate(mt, idx)}
            triple -> {[triple], iterate(mt, idx)}
          end

        idx ->
          {[], iterate(mt, idx)}
      end,
      fn _ -> :ok end
    )
  end

  @spec size(t()) :: non_neg_integer()
  def size(mt), do: :ets.info(mt.tid, :memory) * :erlang.system_info(:wordsize)

  defp get(mt, key, seq) do
    case :ets.lookup(mt.tid, {key, -seq}) do
      [] -> nil
      [{_, value}] -> {key, seq, value}
    end
  end

  defp search_table(mt, key, seq) do
    case :ets.next(mt.tid, {key, -seq}) do
      {k, s} when key == k ->
        [{_, val}] = :ets.lookup(mt, {k, s})
        {key, abs(s), val}

      _ ->
        nil
    end
  end

  defp iterate(mt) do
    idx = :ets.first(mt.tid)
    handle_iteration(mt, idx)
  end

  defp iterate(mt, {key, seq}) do
    idx = :ets.next(mt.tid, {key, -seq})
    handle_iteration(mt, idx)
  end

  defp iterate(mt, idx) do
    idx = :ets.next(mt.tid, idx)
    handle_iteration(mt, idx)
  end

  defp handle_iteration(_mt, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_mt, {key, seq}), do: {key, abs(seq)}
  defp handle_iteration(mt, idx), do: iterate(mt, idx)
end
