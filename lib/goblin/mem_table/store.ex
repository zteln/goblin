defmodule Goblin.MemTable.Store do
  @moduledoc false

  @store_name :mem_table_store

  defstruct [
    :ref,
    :overhead_size
  ]

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new() do
    ref = :ets.new(@store_name, [:ordered_set])
    %__MODULE__{ref: ref, overhead_size: size_of(ref)}
  end

  @spec size(t()) :: non_neg_integer()
  def size(store) do
    size_of(store.ref) - store.overhead_size
  end

  @spec delete(t()) :: :ok
  def delete(store) do
    :ets.delete(store.ref)
    :ok
  end

  @spec insert(t(), term(), term(), keyword()) :: :ok
  def insert(store, key, seq, value) do
    :ets.insert(store.ref, {{key, -seq}, value})
    :ok
  end

  @spec remove(t(), term(), non_neg_integer()) :: :ok
  def remove(store, key, seq) do
    insert(store, key, seq, :"$goblin_tombstone")
  end

  @spec get(t(), term(), non_neg_integer()) :: {term(), non_neg_integer(), term()} | :not_found
  def get(store, key, seq) do
    case :ets.lookup(store.ref, {key, -seq}) do
      [] -> :not_found
      [{{key, seq}, value}] -> {key, abs(seq), value}
    end
  end

  @spec search(t(), term(), non_neg_integer()) :: {term(), non_neg_integer(), term()} | :not_found
  def search(store, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:==, :"$1", {:const, key}}, {:<, {:abs, :"$2"}, seq}}],
        [:"$_"]
      }
    ]

    :ets.select(store.ref, ms)
    |> Enum.map(fn {{key, seq}, value} -> {key, abs(seq), value} end)
    |> Enum.max_by(fn {_key, seq, _value} -> seq end, fn -> :not_found end)
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(store, key) do
    ms = [
      {
        {{:"$1", :_}, :_},
        [{:==, :"$1", {:const, key}}],
        [:"$_"]
      }
    ]

    case :ets.select(store.ref, ms) do
      [] -> false
      _ -> true
    end
  end

  @spec iterate(t()) :: {term(), non_neg_integer()} | :end_of_iteration
  @spec iterate(t(), {term(), non_neg_integer()}) ::
          {term(), non_neg_integer()} | :end_of_iteration
  def iterate(store) do
    idx = :ets.first(store.ref)
    handle_iteration(store, idx)
  end

  def iterate(store, {key, seq}) do
    idx = :ets.next(store.ref, {key, -seq})
    handle_iteration(store, idx)
  end

  def iterate(store, idx) do
    idx = :ets.next(store.ref, idx)
    handle_iteration(store, idx)
  end

  defp handle_iteration(_store, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_store, {key, seq}), do: {key, abs(seq)}
  defp handle_iteration(store, idx), do: iterate(store, idx)

  defp size_of(ref) do
    :ets.info(ref, :memory) * :erlang.system_info(:wordsize)
  end
end
