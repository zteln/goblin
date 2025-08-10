defmodule SeaGoat.MemTable do
  @type t :: :gb_trees.tree()

  @spec new() :: t()
  def new do
    :gb_trees.empty()
  end

  @spec upsert(mem_table :: t(), key :: term(), value :: term()) :: t()
  def upsert(mem_table, _key, :tombstone) do
    mem_table
  end

  def upsert(mem_table, key, value) do
    :gb_trees.enter(key, value, mem_table)
  end

  @spec delete(mem_table :: t(), key :: term()) :: t()
  def delete(mem_table, key) do
    :gb_trees.enter(key, :tombstone, mem_table)
  end

  @spec read(mem_table :: t(), key :: term()) :: term() | nil
  def read(mem_table, key) do
    case :gb_trees.lookup(key, mem_table) do
      {:value, :tombstone} -> nil
      {:value, value} -> {:value, value}
      :none -> nil
    end
  end

  @spec reduce(mem_table :: t(), acc :: term(), reducer :: fun()) :: term()
  def reduce(mem_table, acc, reducer) do
    iter = :gb_trees.iterator(mem_table)
    do_reduce(iter, acc, reducer)
  end

  defp do_reduce(iter, acc, reducer) do
    case :gb_trees.next(iter) do
      :none ->
        acc

      {k, v, iter} ->
        case reducer.(k, v, acc) do
          {:ok, acc} -> do_reduce(iter, acc, reducer)
          {:halt, acc} -> acc
        end
    end
  end

  @spec smallest(mem_table :: t()) :: term()
  def smallest(mem_table) do
    {key, _} = :gb_trees.smallest(mem_table)
    key
  end

  @spec largest(mem_table :: t()) :: term()
  def largest(mem_table) do
    {key, _} = :gb_trees.largest(mem_table)
    key
  end

  @spec keys(mem_table :: t()) :: [term()]
  def keys(mem_table), do: :gb_trees.keys(mem_table)

  @spec size(mem_table :: t()) :: non_neg_integer()
  def size(mem_table), do: :gb_trees.size(mem_table)

  @spec has_overflow(mem_table :: t(), limit :: non_neg_integer()) :: boolean()
  def has_overflow(mem_table, limit), do: :gb_trees.size(mem_table) >= limit
end
