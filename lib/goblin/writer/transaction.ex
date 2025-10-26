defmodule Goblin.Writer.Transaction do
  @moduledoc false
  alias Goblin.Writer.MemTable

  defstruct [
    :owner,
    :fallback_read,
    seq: 0,
    mem_table: MemTable.new(),
    writes: [],
    reads: %{}
  ]

  @type t :: %__MODULE__{}

  @spec new(pid(), (term() -> term())) :: t()
  def new(pid, fallback_read \\ fn _ -> :not_found end) do
    %__MODULE__{owner: pid, fallback_read: fallback_read}
  end

  @spec put(t(), Goblin.db_key(), Goblin.db_value()) :: t()
  def put(tx, key, value) do
    write = {tx.seq, :put, key, value}
    mem_table = MemTable.upsert(tx.mem_table, tx.seq, key, value)
    %{tx | seq: tx.seq + 1, mem_table: mem_table, writes: [write | tx.writes]}
  end

  @spec remove(t(), Goblin.db_key()) :: t()
  def remove(tx, key) do
    write = {tx.seq, :remove, key}
    mem_table = MemTable.delete(tx.mem_table, tx.seq, key)
    %{tx | seq: tx.seq + 1, mem_table: mem_table, writes: [write | tx.writes]}
  end

  @spec get(t(), Goblin.db_key(), term()) :: {Goblin.db_value() | nil, t()}
  def get(tx, key, default \\ nil) do
    read =
      case MemTable.read(tx.mem_table, key) do
        {:value, seq, value} -> {seq, value}
        :not_found -> tx.fallback_read.(key)
      end

    val =
      case read do
        :not_found -> nil
        {_seq, val} -> val
      end

    reads = Map.put(tx.reads, key, read)
    tx = %{tx | reads: reads}
    {val || default, tx}
  end

  @spec has_conflict(t(), [MemTable.t()]) :: boolean()
  def has_conflict(tx, mem_tables) do
    has_read_conflict(tx.reads, mem_tables) || has_write_conflict(tx.mem_table, mem_tables)
  end

  defp has_read_conflict(_, []), do: false

  defp has_read_conflict(reads, [mem_table | mem_tables]) do
    read_conflict? =
      Enum.any?(reads, fn
        {key, :not_found} ->
          case MemTable.read(mem_table, key) do
            :not_found -> false
            {:value, _read_seq, _read_value} -> true
          end

        {key, seq, value} ->
          case MemTable.read(mem_table, key) do
            :not_found -> false
            {:value, read_seq, read_value} -> read_seq != seq and read_value != value
          end
      end)

    if read_conflict?, do: true, else: has_read_conflict(reads, mem_tables)
  end

  defp has_write_conflict(_mem_table, []), do: false

  defp has_write_conflict(mem_table1, [mem_table2 | mem_tables]) do
    if MemTable.is_disjoint(mem_table1, mem_table2) do
      has_write_conflict(mem_table1, mem_tables)
    else
      true
    end
  end
end
