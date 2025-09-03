defmodule SeaGoat.Server.Transaction do
  alias SeaGoat.Server.MemTable

  defstruct mem_table: MemTable.new(),
            merges: []

  def make do
    %__MODULE__{}
  end

  def put(tx, key, value) do
    mem_table = MemTable.upsert(tx.mem_table, key, value)
    %{tx | mem_table: mem_table}
  end

  def remove do
  end

  def read do
  end

  def commit do
  end

  def export do
  end
end
