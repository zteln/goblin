defmodule Goblin.Writer.MemTable do
  @moduledoc false
  @type t :: map()

  @spec new() :: t()
  def new, do: %{}

  @spec upsert(t(), Goblin.db_sequence(), Goblin.db_key(), Goblin.db_value()) :: t()
  def upsert(mem_table, _seq, _key, :tombstone) do
    mem_table
  end

  def upsert(mem_table, seq, key, value) do
    Map.put(mem_table, key, {seq, value})
  end

  @spec delete(t(), Goblin.db_sequence(), Goblin.db_key()) :: t()
  def delete(mem_table, seq, key) do
    Map.put(mem_table, key, {seq, :tombstone})
  end

  @spec read(t(), Goblin.db_key()) :: {:value, nil | Goblin.db_value()} | :not_found
  def read(mem_table, key) do
    case Map.get(mem_table, key) do
      nil -> :not_found
      {seq, :tombstone} -> {:value, seq, nil}
      {seq, value} -> {:value, seq, value}
    end
  end

  @spec sort(t()) :: [{Goblin.db_key(), {Goblin.db_sequence(), Goblin.db_value()}}]
  def sort(mem_table), do: Enum.sort(mem_table)

  @spec flatten(t()) :: [{Goblin.db_sequence(), Goblin.db_key(), Goblin.db_value()}]
  def flatten(mem_table), do: Enum.map(mem_table, fn {key, {seq, value}} -> {seq, key, value} end)

  @spec advance_seq(t(), Goblin.db_sequence()) :: t()
  def advance_seq(mem_table, seq) do
    Enum.into(mem_table, %{}, fn {k, {s, v}} -> {k, {s + seq, v}} end)
  end

  @spec has_overflow(t(), non_neg_integer()) :: boolean()
  def has_overflow(mem_table, limit), do: map_size(mem_table) >= limit

  @spec is_disjoint(t(), t()) :: boolean()
  def is_disjoint(mem_table1, mem_table2) do
    set1 = MapSet.new(Map.keys(mem_table1))
    set2 = MapSet.new(Map.keys(mem_table2))
    MapSet.disjoint?(set1, set2)
  end

  @spec merge(t(), t()) :: t()
  def merge(mem_table1, mem_table2) do
    Map.merge(mem_table1, mem_table2)
  end
end
