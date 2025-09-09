defmodule SeaGoat.Server.Transaction do
  alias SeaGoat.Server.MemTable

  defstruct mem_table: MemTable.new(),
            writes: [],
            owner: nil

  def make(pid) do
    %__MODULE__{owner: pid}
  end

  def put(tx, key, value) do
    write = {:put, key, value}
    mem_table = MemTable.upsert(tx.mem_table, key, value)
    %{tx | mem_table: mem_table, writes: [write | tx.writes]}
  end

  def remove(tx, key) do
    write = {:remove, key}
    mem_table = MemTable.delete(tx.mem_table, key)
    %{tx | mem_table: mem_table, writes: [write | tx.writes]}
  end

  def read(tx, key) do
    case MemTable.read(tx.mem_table, key) do
      {:value, value} -> value
      nil -> nil
    end
  end

  def is_in_conflict(_, []), do: false

  def is_in_conflict(tx, [committed | commits]) do
    MemTable.is_disjoint(tx.mem_table, committed) && is_in_conflict(tx, commits)
  end
end
