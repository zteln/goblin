defmodule SeaGoat.Writer.Transaction do
  @moduledoc """
  A transaction keeps an internal MemTable that is merged into the main MemTable when committed as long as it is conflict-free.
  The transaction tracks what is written and read and checks for eventual conflicts.

  There are two possible conflicts:
  1. read conflict: the transaction reads a value corresponding to a key that has changed since the read. E.g. transaction A reads `key` with value `val1`, then transaction B writes `val2` to `key` and commits. Transaction A has then a read conflict.
  2. write conflict: a concurrent transaction commits a change to a key before this transaction. E.g. two transactions, A and B, write to `key` values `val1` and `val2`, respectively. A commits before B, then B has a write conflict.

  Reading a key in a transaction first searches for the key-value pair in its internal MemTable before calling the `:fallback_read` function.
  """
  alias SeaGoat.Writer.MemTable

  defstruct [
    :owner,
    :fallback_read,
    seq: 0,
    mem_table: MemTable.new(),
    writes: [],
    reads: %{}
  ]

  @type t :: %__MODULE__{}

  @doc """
  Returns a new `Transaction` structure.
  Each transaction keeps track of its owner (the process executing the transaction), the writer (the process that commits the transactions), and the store (the process responsible for on-disk storage).
  The writer pid is used for reading from the in-memory MemTable.
  The store pid is used to reading the on-disk SSTables.
  """
  @spec new(pid(), (term() -> term())) :: t()
  def new(pid, fallback_read \\ fn _ -> :not_found end) do
    %__MODULE__{owner: pid, fallback_read: fallback_read}
  end

  @doc """
  Puts the key-value pair in the transactions MemTable, overriding `key` if it exists.
  """
  @spec put(t(), SeaGoat.db_key(), SeaGoat.db_value()) :: t()
  def put(tx, key, value) do
    write = {tx.seq, :put, key, value}
    mem_table = MemTable.upsert(tx.mem_table, tx.seq, key, value)
    %{tx | seq: tx.seq + 1, mem_table: mem_table, writes: [write | tx.writes]}
  end

  @doc """
  Removes `key` and corresponding value in the transactions MemTable.
  """
  @spec remove(t(), SeaGoat.db_key()) :: t()
  def remove(tx, key) do
    write = {tx.seq, :remove, key}
    mem_table = MemTable.delete(tx.mem_table, tx.seq, key)
    %{tx | seq: tx.seq + 1, mem_table: mem_table, writes: [write | tx.writes]}
  end

  @doc """
  Checks whether the transaction has any conflicts with other MemTables.
  A conflict can arise two ways:
  - if the transaction has read a key with a value differing to the value found under the same in one of the MemTables.
  - if the transaction attempts to write to a key already existing in one of the MemTables.
  """
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

  @doc """
  Reads `key` from either its own MemTable or via its `fallback_read` function if `:not_found` is returned from its own MemTable..
  """
  @spec get(t(), SeaGoat.db_key()) :: {{SeaGoat.db_sequence(), SeaGoat.db_value()} | :error, t()}
  def get(tx, key) do
    read =
      case MemTable.read(tx.mem_table, key) do
        {:value, seq, value} -> {seq, value}
        :not_found -> tx.fallback_read.(key)
      end

    reads = Map.put(tx.reads, key, read)
    tx = %{tx | reads: reads}
    {read, tx}
  end
end
