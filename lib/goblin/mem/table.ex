defmodule Goblin.Mem.Table do
  @moduledoc false

  @table_name :mem_table

  defstruct [
    :ref,
    :overhead_size
  ]

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new() do
    ref = :ets.new(@table_name, [:ordered_set])
    %__MODULE__{ref: ref, overhead_size: size_of(ref)}
  end

  @spec size(t()) :: non_neg_integer()
  def size(table) do
    size_of(table.ref) - table.overhead_size
  end

  @spec delete(t()) :: :ok
  def delete(table) do
    :ets.delete(table.ref)
    :ok
  end

  @spec insert(t(), term(), term(), keyword()) :: :ok
  def insert(table, key, seq, value) do
    :ets.insert(table.ref, {{key, -seq}, value})
    :ok
  end

  @spec remove(t(), term(), non_neg_integer()) :: :ok
  def remove(table, key, seq) do
    insert(table, key, seq, :"$goblin_tombstone")
  end

  @spec get(t(), term(), non_neg_integer()) :: {term(), non_neg_integer(), term()} | :not_found
  def get(table, key, seq) do
    case :ets.lookup(table.ref, {key, -seq}) do
      [] -> :not_found
      [{{key, seq}, value}] -> {key, abs(seq), value}
    end
  end

  @spec search(t(), term(), :infinity | non_neg_integer()) ::
          {term(), non_neg_integer(), term()} | :not_found
  def search(table, key, seq) do
    case :ets.next(table.ref, {key, -seq}) do
      {k, s} when key == k ->
        [{_, value}] = :ets.lookup(table.ref, {k, s})
        {key, abs(s), value}

      _ ->
        :not_found
    end
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(table, key) do
    case :ets.prev(table.ref, {key, 1}) do
      {k, _} when k == key -> true
      _ -> false
    end
  end

  @spec iterate(t()) :: {term(), non_neg_integer()} | :end_of_iteration
  @spec iterate(t(), {term(), non_neg_integer()}) ::
          {term(), non_neg_integer()} | :end_of_iteration
  def iterate(table) do
    idx = :ets.first(table.ref)
    handle_iteration(table, idx)
  end

  def iterate(table, {key, seq}) do
    idx = :ets.next(table.ref, {key, -seq})
    handle_iteration(table, idx)
  end

  def iterate(table, idx) do
    idx = :ets.next(table.ref, idx)
    handle_iteration(table, idx)
  end

  defp handle_iteration(_table, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_table, {key, seq}), do: {key, abs(seq)}
  defp handle_iteration(table, idx), do: iterate(table, idx)

  defp size_of(ref) do
    :ets.info(ref, :memory) * :erlang.system_info(:wordsize)
  end
end
