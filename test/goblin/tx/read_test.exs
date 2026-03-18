defmodule Goblin.Tx.ReadTest do
  use ExUnit.Case, async: true

  alias Goblin.Tx.Read
  alias Goblin.Transactionable

  defp new_tx(opts \\ []) do
    Read.new(
      opts[:name] || :test,
      opts[:tx_key] || make_ref(),
      opts[:seq] || 0,
      opts[:max_level_key] || -1
    )
  end

  describe "new/4" do
    test "creates a read tx" do
      tx = new_tx(name: :db, seq: 10, max_level_key: 3)

      assert %Read{} = tx
      assert tx.name == :db
      assert tx.seq == 10
      assert tx.max_level_key == 3
    end
  end

  describe "write operations raise" do
    test "put/4 raises on a read transaction" do
      tx = new_tx()

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Transactionable.put(tx, :key, :val, [])
      end
    end

    test "remove/3 raises on a read transaction" do
      tx = new_tx()

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Transactionable.remove(tx, :key, [])
      end
    end

    test "put_multi/3 raises on a read transaction" do
      tx = new_tx()

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Transactionable.put_multi(tx, [{:a, 1}], [])
      end
    end

    test "remove_multi/3 raises on a read transaction" do
      tx = new_tx()

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Transactionable.remove_multi(tx, [:a], [])
      end
    end
  end

  describe "search/6" do
    test "returns empty list when no keys match" do
      tables_f = fn _level_key -> [] end

      assert [] == Read.search(tables_f, [:missing], 10, -1)
    end

    test "returns empty list for empty keys" do
      tables_f = fn _level_key -> [] end

      assert [] == Read.search(tables_f, [], 10, 0)
    end
  end
end
