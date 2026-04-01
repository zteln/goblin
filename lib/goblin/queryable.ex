defprotocol Goblin.Queryable do
  @moduledoc false
  @type t :: t()

  @spec has_key?(t(), Goblin.db_key()) :: boolean()
  def has_key?(table, key)

  @spec search(t(), [Goblin.db_key()], Goblin.seq_no()) :: Goblin.Iterable.t()
  def search(table, keys, seq)

  @spec stream(t(), Goblin.db_key() | nil, Goblin.db_key() | nil, Goblin.seq_no()) ::
          Goblin.Iterable.t()
  def stream(table, min, max, seq)
end

defimpl Goblin.Queryable, for: List do
  def has_key?(list, key), do: Enum.any?(list, fn {k, _s, _v} -> k == key end)

  def search(list, keys, seq),
    do: Enum.filter(list, fn {k, s, _v} -> s < seq and k in keys end)

  def stream(list, nil, nil, seq),
    do: Enum.filter(list, fn {_k, s, _v} -> s < seq end)

  def stream(list, min, nil, seq),
    do: Enum.filter(list, fn {k, s, _v} -> min <= k and s < seq end)

  def stream(list, nil, max, seq),
    do: Enum.filter(list, fn {k, s, _v} -> k <= max and s < seq end)

  def stream(list, min, max, seq),
    do: Enum.filter(list, fn {k, s, _v} -> min <= k and k <= max and s < seq end)
end
