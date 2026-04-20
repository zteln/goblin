defmodule Goblin.TxTest do
  use ExUnit.Case, async: true

  alias Goblin.Tx

  defp new_tx(opts \\ []) do
    %Tx{
      mode: opts[:mode] || :write,
      sequence: opts[:sequence] || 0,
      tx_id: opts[:tx_id] || 0,
      broker: opts[:broker],
      max_level_key: opts[:max_level_key] || -1
    }
  end

  describe "put/4 (write mode)" do
    test "appends a {key, seq, value} triple and increments sequence" do
      tx =
        new_tx(sequence: 0)
        |> Tx.put(:key, :val, [])

      assert tx.sequence == 1
      assert tx.writes == [{:key, 0, :val}]
    end

    test "tag option wraps the key" do
      tx =
        new_tx(sequence: 0)
        |> Tx.put(:key, :val, tag: :users)

      assert tx.writes == [{{:"$goblin_tag", :users, :key}, 0, :val}]
    end
  end

  describe "put_multi/3 (write mode)" do
    test "appends triples in insertion order (reversed since prepended)" do
      tx =
        new_tx(sequence: 0)
        |> Tx.put_multi([{:a, 1}, {:b, 2}], [])

      assert tx.sequence == 2
      assert tx.writes == [{:b, 1, 2}, {:a, 0, 1}]
    end
  end

  describe "remove/3 (write mode)" do
    test "appends a tombstone triple" do
      tx =
        new_tx(sequence: 0)
        |> Tx.remove(:key, [])

      assert tx.sequence == 1
      assert tx.writes == [{:key, 0, :"$goblin_tombstone"}]
    end

    test "tag option wraps the key on tombstone" do
      tx =
        new_tx(sequence: 0)
        |> Tx.remove(:key, tag: :users)

      assert tx.writes == [{{:"$goblin_tag", :users, :key}, 0, :"$goblin_tombstone"}]
    end
  end

  describe "remove_multi/3 (write mode)" do
    test "appends multiple tombstones" do
      tx =
        new_tx(sequence: 0)
        |> Tx.remove_multi([:a, :b], [])

      assert tx.sequence == 2

      assert tx.writes == [
               {:b, 1, :"$goblin_tombstone"},
               {:a, 0, :"$goblin_tombstone"}
             ]
    end
  end

  describe "read-mode protection" do
    test "put/4 raises on a read tx" do
      tx = new_tx(mode: :read)

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Tx.put(tx, :key, :val, [])
      end
    end

    test "put_multi/3 raises on a read tx" do
      tx = new_tx(mode: :read)

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Tx.put_multi(tx, [{:a, 1}], [])
      end
    end

    test "remove/3 raises on a read tx" do
      tx = new_tx(mode: :read)

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Tx.remove(tx, :key, [])
      end
    end

    test "remove_multi/3 raises on a read tx" do
      tx = new_tx(mode: :read)

      assert_raise RuntimeError, "Operation not allowed during read", fn ->
        Tx.remove_multi(tx, [:a], [])
      end
    end
  end

  describe "commit/2 and abort/1" do
    test "commit/2 returns {:commit, tx, :ok} with writes in prepend order" do
      tx =
        new_tx(sequence: 0)
        |> Tx.put(:a, 1, [])
        |> Tx.put(:b, 2, [])

      assert {:commit, committed, :ok} = Tx.commit(tx)
      # writes stay in prepend order (newest-first); commit does not reverse
      assert committed.writes == [{:b, 1, 2}, {:a, 0, 1}]
    end

    test "commit/2 accepts a custom reply" do
      tx = new_tx(sequence: 0)

      assert {:commit, _tx, :my_reply} = Tx.commit(tx, :my_reply)
    end

    test "abort/1 returns :abort" do
      assert :abort == Tx.abort(new_tx())
    end
  end
end
