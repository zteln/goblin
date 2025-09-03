defmodule SeaGoat.Server.MemTable do
  @type t :: map()

  @spec new() :: t()
  def new do
    %{}
  end

  @spec upsert(mem_table :: t(), key :: term(), value :: term()) :: t()
  def upsert(mem_table, _key, :tombstone) do
    mem_table
  end

  def upsert(mem_table, key, value) do
    Map.put(mem_table, key, value)
  end

  @spec delete(mem_table :: t(), key :: term()) :: t()
  def delete(mem_table, key) do
    Map.put(mem_table, key, :tombstone)
  end

  @spec read(mem_table :: t(), key :: term()) :: term() | nil
  def read(mem_table, key) do
    case Map.get(mem_table, key) do
      nil -> nil
      :tombstone -> nil
      value -> {:value, value}
    end
  end

  @spec smallest(mem_table :: t()) :: term()
  def smallest(mem_table) do
    {key, _} = Enum.min(mem_table)
    key
  end

  @spec largest(mem_table :: t()) :: term()
  def largest(mem_table) do
    {key, _} = Enum.max(mem_table)
    key
  end

  @spec keys(mem_table :: t()) :: [term()]
  def keys(mem_table), do: Map.keys(mem_table)

  @spec size(mem_table :: t()) :: non_neg_integer()
  def size(mem_table), do: map_size(mem_table)

  @spec has_overflow(mem_table :: t(), limit :: non_neg_integer()) :: boolean()
  def has_overflow(mem_table, limit), do: size(mem_table) >= limit
end
