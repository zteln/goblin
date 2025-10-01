defmodule SeaGoat.Writer.MemTable do
  @moduledoc """
  MemTables are the in-memory data structure that writes are buffered to before being flushed to disk.
  
  MemTables store key-value pairs as a simple map, with special handling for deleted keys
  using tombstone markers to prevent resurrection from on-disk data.
  """
  @type t :: map()

  @doc """
  Returns a new empty MemTable.
  
  ## Examples
  
      iex> MemTable.new()
      %{}
  """
  @spec new() :: t()
  def new, do: %{}

  @doc """
  Updates or inserts a key-value pair into the MemTable.
  
  If the `key` already exists, it is overwritten with `value`.
  The special value `:tombstone` cannot be inserted and will return 
  the unchanged MemTable if attempted.
  
  ## Examples
  
      iex> MemTable.upsert(%{}, "key", "value")
      %{"key" => "value"}
      
      iex> MemTable.upsert(%{"key" => "old"}, "key", "new") 
      %{"key" => "new"}
      
      iex> MemTable.upsert(%{}, "key", :tombstone)
      %{}
  """
  @spec upsert(t(), SeaGoat.db_key(), SeaGoat.db_value()) :: t()
  def upsert(mem_table, _key, :tombstone) do
    mem_table
  end

  def upsert(mem_table, key, value) do
    Map.put(mem_table, key, value)
  end

  @doc """
  Marks a key as deleted by setting its value to `:tombstone`.
  
  The key-value pair is not removed entirely. Instead, the value is set to `:tombstone`
  to prevent the key from being resurrected by on-disk SSTables that might contain 
  a value for this key.
  
  ## Examples
  
      iex> MemTable.delete(%{}, "key")
      %{"key" => :tombstone}
      
      iex> MemTable.delete(%{"key" => "value"}, "key")  
      %{"key" => :tombstone}
  """
  @spec delete(t(), SeaGoat.db_key()) :: t()
  def delete(mem_table, key) do
    Map.put(mem_table, key, :tombstone)
  end

  @doc """
  Reads the value for a key in the MemTable.
  
  Returns:
  - `{:value, value}` if the key exists with a regular value
  - `{:value, nil}` if the key has been deleted (tombstoned)
  - `:not_found` if the key does not exist in the MemTable
  
  ## Examples
  
      iex> MemTable.read(%{"key" => "value"}, "key")
      {:value, "value"}
      
      iex> MemTable.read(%{"key" => :tombstone}, "key")
      {:value, nil}
      
      iex> MemTable.read(%{}, "missing")
      :not_found
  """
  @spec read(t(), SeaGoat.db_key()) :: {:value, nil | SeaGoat.db_value()} | :not_found
  def read(mem_table, key) do
    case Map.get(mem_table, key) do
      nil -> :not_found
      :tombstone -> {:value, nil}
      value -> {:value, value}
    end
  end

  @doc """
  Checks whether the MemTable has exceeded the specified size limit.
  
  Returns `true` if the number of keys is greater than or equal to the limit.
  
  ## Examples
  
      iex> MemTable.has_overflow(%{}, 10)
      false
      
      iex> MemTable.has_overflow(%{"k1" => "v1", "k2" => "v2"}, 1)
      true
  """
  @spec has_overflow(t(), non_neg_integer()) :: boolean()
  def has_overflow(mem_table, limit), do: map_size(mem_table) >= limit

  @doc """
  Checks whether two MemTables have completely different key sets.
  
  Returns `true` if the MemTables share no common keys, `false` otherwise.
  
  ## Examples
  
      iex> MemTable.is_disjoint(%{"a" => 1}, %{"b" => 2})
      true
      
      iex> MemTable.is_disjoint(%{"a" => 1}, %{"a" => 2})  
      false
  """
  @spec is_disjoint(t(), t()) :: boolean()
  def is_disjoint(mem_table1, mem_table2) do
    set1 = MapSet.new(Map.keys(mem_table1))
    set2 = MapSet.new(Map.keys(mem_table2))
    MapSet.disjoint?(set1, set2)
  end

  @doc """
  Merges two MemTables into a single MemTable.
  
  ## Examples
  
      iex> MemTable.merge(%{"a" => 1}, %{"b" => 2})
      %{"a" => 1, "b" => 2}
  """
  @spec merge(t(), t()) :: t()
  def merge(mem_table1, mem_table2) do
    Map.merge(mem_table1, mem_table2)
  end
end
