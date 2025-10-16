defmodule SeaGoat.Writer.TransactionTest do
  use ExUnit.Case, async: true
  alias SeaGoat.Writer.Transaction
  alias SeaGoat.Writer.MemTable

  test "new/3 returns new Transaction struct" do
    assert %Transaction{
             owner: :owner,
             mem_table: %{},
             reads: %{},
             writes: []
           } = Transaction.new(:owner)
  end

  test "put/3 writes to MemTable and inserts a write command" do
    tx = Transaction.new(:owner)

    assert %Transaction{writes: [{0, :put, :k, :v}], mem_table: %{k: {0, :v}}} =
             tx =
             Transaction.put(tx, :k, :v)

    assert %Transaction{writes: [{1, :put, :k, :w}, {0, :put, :k, :v}], mem_table: %{k: {1, :w}}} =
             Transaction.put(tx, :k, :w)
  end

  test "remove/2 writes to MemTable and inserts a write command" do
    tx = Transaction.new(:owner) |> Transaction.put(:k, :v)

    assert %Transaction{
             writes: [{1, :remove, :k}, {0, :put, :k, :v}],
             mem_table: %{k: {1, :tombstone}}
           } =
             Transaction.remove(tx, :k)
  end

  test "remove/2 removes key not written during transaction" do
    tx = Transaction.new(:owner)

    assert %Transaction{writes: [{0, :remove, :k}], mem_table: %{k: {0, :tombstone}}} =
             Transaction.remove(tx, :k)
  end

  test "has_conflict/2 returns true on read conflict" do
    {nil, tx} = Transaction.new(:owner) |> Transaction.get(:k3)

    mem_tables = [
      MemTable.upsert(MemTable.new(), 0, :k1, :v1),
      MemTable.upsert(MemTable.new(), 1, :k2, :v1),
      MemTable.upsert(MemTable.new(), 2, :k3, :v1)
    ]

    assert Transaction.has_conflict(tx, mem_tables)
  end

  test "has_conflict/2 returns true on write conflict" do
    tx =
      Transaction.new(:owner)
      |> Transaction.put(:k1, :v0)
      |> Transaction.put(:k2, :v0)
      |> Transaction.put(:k3, :v0)

    mem_tables = [
      MemTable.upsert(MemTable.new(), 0, :k1, :v1),
      MemTable.upsert(MemTable.new(), 1, :k2, :v1),
      MemTable.upsert(MemTable.new(), 2, :k3, :v1)
    ]

    assert Transaction.has_conflict(tx, mem_tables)
  end

  test "has_conflict/2 returns false on no conflicts" do
    {nil, tx} =
      Transaction.new(:owner)
      |> Transaction.put(:k1, :v0)
      |> Transaction.get(:k2)

    mem_table = MemTable.new() |> MemTable.upsert(0, :k3, :v1)
    refute Transaction.has_conflict(tx, [mem_table])
  end

  test "get/2 updates read table and returns value" do
    tx = Transaction.new(:owner) |> Transaction.put(:k, :v)
    assert {{0, :v}, %{reads: %{k: {0, :v}}}} = Transaction.get(tx, :k)
  end

  test "get/2 returns value from fallback reader" do
    tx = Transaction.new(:owner, & &1)
    assert {:k, %{reads: %{k: :k}}} = Transaction.get(tx, :k)
  end
end
