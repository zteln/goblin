defmodule Goblin.Writer.TransactionTest do
  use ExUnit.Case, async: true
  alias Goblin.Writer.Transaction

  describe "put/3" do
    test "prepends to writes in transaction" do
      assert %{writes: [], seq: 0} = tx = Transaction.new(0)
      assert %{writes: [{:put, 0, :k1, :v1}], seq: 1} = tx = Transaction.put(tx, :k1, :v1)

      assert %{writes: [{:put, 1, :k2, :v2}, {:put, 0, :k1, :v1}], seq: 2} =
               Transaction.put(tx, :k2, :v2)
    end
  end

  describe "remove/2" do
    test "prepends to writes in transaction" do
      assert %{writes: [], seq: 0} = tx = Transaction.new(0)
      assert %{writes: [{:remove, 0, :k1}], seq: 1} = tx = Transaction.remove(tx, :k1)

      assert %{writes: [{:remove, 1, :k2}, {:remove, 0, :k1}], seq: 2} =
               Transaction.remove(tx, :k2)
    end
  end

  describe "get/3" do
    test "reads from tx's writes" do
      tx = Transaction.new(0)
      tx = Transaction.put(tx, :k1, :v1)
      tx = Transaction.remove(tx, :k2)

      assert :v1 == Transaction.get(tx, :k1)
      assert nil == Transaction.get(tx, :k2)
      assert :default == Transaction.get(tx, :k2, :default)
    end

    test "fallback read is called if not found in tx's writes" do
      tx = Transaction.new(0, fn _key -> {0, :from_fallback_read} end)
      assert :from_fallback_read == Transaction.get(tx, :k1)
    end
  end
end
