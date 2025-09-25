defmodule SeaGoat.Writer.MemTable do
  @moduledoc """
  MemTables are the in-memory data structure that writes are buffered to before being flushed to disk.
  """
  @type t :: map()

  @doc """
  Returns a new MemTable.
  """
  @spec new() :: t()
  def new do
    %{}
  end

  @doc """
  Updates or inserts a key-value pair.
  If the `key` already exists, then it is overwritten with `value`.
  `:tombstone` is a reserved value and attempting to upsert with it will return the same, unchanged mem-table.
  """
  @spec upsert(t(), term(), term()) :: t()
  def upsert(mem_table, _key, :tombstone) do
    mem_table
  end

  def upsert(mem_table, key, value) do
    Map.put(mem_table, key, value)
  end

  @doc """
  Deletes a key-value pair.
  The key-value pair is not removed entirely, instead the value is set to `:tombstone`.
  The reason for this is so that `key` is not resurrected by the on-disk SSTables that might hold a value for `key`.
  """
  @spec delete(t(), term()) :: t()
  def delete(mem_table, key) do
    Map.put(mem_table, key, :tombstone)
  end

  @doc """
  Reads the value for a key in the mem-table.
  Returns `{:value, value}` if the key exists, where `value` is `nil` if the key has been deleted.
  If the key does not exist, then `:not_found` is returned.
  """
  @spec read(t(), term()) :: {:value, nil | term()} | :not_found
  def read(mem_table, key) do
    case Map.get(mem_table, key) do
      nil -> :not_found
      :tombstone -> {:value, nil}
      value -> {:value, value}
    end
  end

  @doc """
  Checks whether the mem-table has an overflow, i.e. if the number of keys exceeds `limit`.
  """
  @spec has_overflow(t(), non_neg_integer()) :: boolean()
  def has_overflow(mem_table, limit), do: map_size(mem_table) >= limit

  @doc """
  Checks whether two mem-tables have any shared keys.
  """
  @spec is_disjoint(t(), t()) :: boolean()
  def is_disjoint(mem_table1, mem_table2) do
    set1 = MapSet.new(Map.keys(mem_table1))
    set2 = MapSet.new(Map.keys(mem_table2))
    MapSet.disjoint?(set1, set2)
  end

  @doc """
  Merges two mem-tables. `mem_table2` overwrites any keys in `mem_table1`.
  """
  @spec merge(t(), t()) :: t()
  def merge(mem_table1, mem_table2) do
    Map.merge(mem_table1, mem_table2)
  end
end
