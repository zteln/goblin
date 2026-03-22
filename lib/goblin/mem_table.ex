defmodule Goblin.MemTable do
  @moduledoc false

  @mem_table_name :mem_table

  defstruct [
    :table,
    :overhead_size
  ]

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new() do
    table = :ets.new(@mem_table_name, [:ordered_set])
    %__MODULE__{table: table, overhead_size: size_of(table)}
  end

  @spec size(t()) :: non_neg_integer()
  def size(mem_table) do
    size_of(mem_table.table) - mem_table.overhead_size
  end

  @spec delete(t()) :: :ok
  def delete(mem_table) do
    :ets.delete(mem_table.table)
    :ok
  end

  @spec insert(t(), term(), term(), keyword()) :: :ok
  def insert(mem_table, key, seq, value) do
    :ets.insert(mem_table.table, {{key, -seq}, value})
    :ok
  end

  @spec remove(t(), term(), non_neg_integer()) :: :ok
  def remove(mem_table, key, seq) do
    insert(mem_table, key, seq, :"$goblin_tombstone")
  end

  @spec get(t(), term(), non_neg_integer()) :: {term(), non_neg_integer(), term()} | :not_found
  def get(mem_table, key, seq) do
    case :ets.lookup(mem_table.table, {key, -seq}) do
      [] -> :not_found
      [{{key, seq}, value}] -> {key, abs(seq), value}
    end
  end

  @spec search(t(), term(), non_neg_integer()) :: {term(), non_neg_integer(), term()} | :not_found
  def search(mem_table, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:==, :"$1", {:const, key}}, {:<, {:abs, :"$2"}, seq}}],
        [:"$_"]
      }
    ]

    :ets.select(mem_table.table, ms)
    |> Enum.map(fn {{key, seq}, value} -> {key, abs(seq), value} end)
    |> Enum.max_by(fn {_key, seq, _value} -> seq end, fn -> :not_found end)
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(mem_table, key) do
    ms = [
      {
        {{:"$1", :_}, :_},
        [{:==, :"$1", {:const, key}}],
        [:"$_"]
      }
    ]

    case :ets.select(mem_table.table, ms) do
      [] -> false
      _ -> true
    end
  end

  @spec iterate(t()) :: {term(), non_neg_integer()} | :end_of_iteration
  @spec iterate(t(), {term(), non_neg_integer()}) ::
          {term(), non_neg_integer()} | :end_of_iteration
  def iterate(mem_table) do
    idx = :ets.first(mem_table.table)
    handle_iteration(mem_table, idx)
  end

  def iterate(mem_table, {key, seq}) do
    idx = :ets.next(mem_table.table, {key, -seq})
    handle_iteration(mem_table, idx)
  end

  def iterate(mem_table, idx) do
    idx = :ets.next(mem_table.table, idx)
    handle_iteration(mem_table, idx)
  end

  defp handle_iteration(_mem_table, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_mem_table, {key, seq}), do: {key, abs(seq)}
  defp handle_iteration(mem_table, idx), do: iterate(mem_table, idx)

  defp size_of(table) do
    :ets.info(table, :memory) * :erlang.system_info(:wordsize)
  end
end

defimpl Goblin.Queryable, for: Goblin.MemTable do
  alias Goblin.MemTable

  def has_key?(mem_table, key) do
    MemTable.has_key?(mem_table, key)
  end

  def search(mem_table, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case MemTable.search(mem_table, key, seq) do
        {_key, _seq, _value} = triple -> [triple]
        _ -> []
      end
    end)
  end

  def stream(mem_table, _min, _max, seq) do
    %MemTable.Iterator{mem_table: mem_table, max_seq: seq}
  end
end
