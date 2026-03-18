defmodule Goblin.Tx.WriteTest do
  use ExUnit.Case, async: true

  alias Goblin.Tx.Write
  alias Goblin.Transactionable

  defp new_tx(opts) do
    Write.new(
      opts[:name] || :test,
      opts[:tx_key] || make_ref(),
      opts[:seq] || 0,
      opts[:max_level_key] || -1
    )
  end

  describe "new/4" do
    test "creates a write tx with empty writes" do
      tx = new_tx(name: :db, seq: 5, max_level_key: 2)

      assert %Write{} = tx
      assert tx.name == :db
      assert tx.seq == 5
      assert tx.max_level_key == 2
      assert tx.writes == []
    end
  end

  describe "put/4" do
    test "accumulates a write and increments seq" do
      tx =
        new_tx(seq: 0)
        |> Transactionable.put(:key, :val, [])

      assert tx.seq == 1
      assert tx.writes == [{:put, 0, :key, :val}]
    end
  end

  describe "put_multi/3" do
    test "accumulates multiple writes" do
      tx =
        new_tx(seq: 0)
        |> Transactionable.put_multi([{:a, 1}, {:b, 2}], [])

      assert tx.seq == 2
      # writes are prepended, so reversed order
      assert tx.writes == [{:put, 1, :b, 2}, {:put, 0, :a, 1}]
    end
  end

  describe "remove/3" do
    test "accumulates a remove write and increments seq" do
      tx =
        new_tx(seq: 0)
        |> Transactionable.remove(:key, [])

      assert tx.seq == 1
      assert tx.writes == [{:remove, 0, :key}]
    end
  end

  describe "remove_multi/3" do
    test "accumulates multiple remove writes" do
      tx =
        new_tx(seq: 0)
        |> Transactionable.remove_multi([:a, :b], [])

      assert tx.seq == 2
      assert tx.writes == [{:remove, 1, :b}, {:remove, 0, :a}]
    end
  end

  describe "complete/1" do
    test "reverses the writes list" do
      tx =
        new_tx(seq: 0)
        |> Transactionable.put(:a, 1, [])
        |> Transactionable.put(:b, 2, [])
        |> Write.complete()

      assert tx.writes == [{:put, 0, :a, 1}, {:put, 1, :b, 2}]
    end
  end

  describe "tag option" do
    test "wraps key in $goblin_tag tuple" do
      tx =
        new_tx(seq: 0)
        |> Transactionable.put(:key, :val, tag: :users)

      assert tx.writes == [{:put, 0, {:"$goblin_tag", :users, :key}, :val}]

      tx =
        new_tx(seq: 0)
        |> Transactionable.remove(:key, tag: :users)

      assert tx.writes == [{:remove, 0, {:"$goblin_tag", :users, :key}}]
    end
  end
end
