defmodule Goblin.MemTable.StoreTest do
  use ExUnit.Case, async: true

  setup do
    store = Goblin.MemTable.Store.new(__MODULE__)
    %{store: store}
  end

  describe "insert_commit_seq/2, get_commit_seq/1" do
    test "default sequence is 0", c do
      assert 0 == Goblin.MemTable.Store.get_commit_seq(c.store)
    end

    test "can insert and read committed sequence", c do
      assert :ok == Goblin.MemTable.Store.insert_commit_seq(c.store, 1)
      assert 1 == Goblin.MemTable.Store.get_commit_seq(c.store)
      assert :ok == Goblin.MemTable.Store.insert_commit_seq(c.store, 2000)
      assert 2000 == Goblin.MemTable.Store.get_commit_seq(c.store)
    end
  end

  describe "insert/4, remove/3, get/3" do
    test "can read and update entries", c do
      assert :not_found == Goblin.MemTable.Store.get(c.store, :key, 0)
      assert :ok == Goblin.MemTable.Store.insert(c.store, :key, 0, :val)
      assert {:key, 0, :val} == Goblin.MemTable.Store.get(c.store, :key, 0)
      assert :ok == Goblin.MemTable.Store.remove(c.store, :key, 1)
      assert {:key, 1, :"$goblin_tombstone"} == Goblin.MemTable.Store.get(c.store, :key, 1)
    end
  end

  describe "get_by_key/3" do
    test "gets highest entry below provided sequence", c do
      assert :not_found == Goblin.MemTable.Store.get_by_key(c.store, :key, 0)
      Goblin.MemTable.Store.insert(c.store, :key, 0, :val1)
      Goblin.MemTable.Store.insert(c.store, :key, 1, :val2)
      Goblin.MemTable.Store.insert(c.store, :key, 2, :val3)
      assert {:key, 0, :val1} == Goblin.MemTable.Store.get_by_key(c.store, :key, 1)
      assert {:key, 1, :val2} == Goblin.MemTable.Store.get_by_key(c.store, :key, 2)
      assert {:key, 2, :val3} == Goblin.MemTable.Store.get_by_key(c.store, :key, 3)
    end

    test "gets entry for key with same value (i.e. key1 == key2 only)", c do
      key1 = 0
      key2 = 0.0
      assert key1 == key2
      refute key1 === key2

      Goblin.MemTable.Store.insert(c.store, key1, 0, :val1)
      Goblin.MemTable.Store.insert(c.store, key2, 1, :val2)

      assert {key1, 0, :val1} == Goblin.MemTable.Store.get_by_key(c.store, key1, 1)
      assert {key2, 1, :val2} == Goblin.MemTable.Store.get_by_key(c.store, key1, 2)

      assert {key1, 0, :val1} == Goblin.MemTable.Store.get_by_key(c.store, key2, 1)
      assert {key2, 1, :val2} == Goblin.MemTable.Store.get_by_key(c.store, key2, 2)
    end
  end

  describe "delete_range/2" do
    test "deletes entries below provided sequence", c do
      Goblin.MemTable.Store.insert(c.store, :key1, 0, :val1)
      Goblin.MemTable.Store.insert(c.store, :key2, 1, :val2)
      Goblin.MemTable.Store.insert(c.store, :key3, 2, :val3)
      assert {:key1, 0, :val1} == Goblin.MemTable.Store.get_by_key(c.store, :key1, 3)
      assert {:key2, 1, :val2} == Goblin.MemTable.Store.get_by_key(c.store, :key2, 3)
      assert {:key3, 2, :val3} == Goblin.MemTable.Store.get_by_key(c.store, :key3, 3)
      assert :ok == Goblin.MemTable.Store.delete_range(c.store, 3)
      assert :not_found == Goblin.MemTable.Store.get_by_key(c.store, :key1, 3)
      assert :not_found == Goblin.MemTable.Store.get_by_key(c.store, :key2, 3)
      assert :not_found == Goblin.MemTable.Store.get_by_key(c.store, :key3, 3)
    end
  end

  describe "iterate/1/2" do
    test "iteration is ordered through the store", c do
      data = [
        {:key1, 0, :val1},
        {:key2, 1, :val2},
        {:key3, 2, :val3},
        {:key1, 3, :val1_1}
      ]

      for {key, seq, val} <- data,
          do: Goblin.MemTable.Store.insert(c.store, key, seq, val)

      data
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
      |> Enum.reduce(Goblin.MemTable.Store.iterate(c.store), fn triple, {key, seq} ->
        assert triple == Goblin.MemTable.Store.get(c.store, key, seq)
        Goblin.MemTable.Store.iterate(c.store, {key, seq})
      end)
    end

    test "iteration can end", c do
      assert :end_of_iteration == Goblin.MemTable.Store.iterate(c.store)
      Goblin.MemTable.Store.insert(c.store, :key, 0, :val)
      next = Goblin.MemTable.Store.iterate(c.store)
      assert :end_of_iteration == Goblin.MemTable.Store.iterate(c.store, next)
    end
  end

  describe "inc_streamers/1, deinc_streamers/1, get_streamers_count/1" do
    test "anyone can inc and deinc streamer count", c do
      parent = self()
      assert 0 == Goblin.MemTable.Store.get_streamers_count(c.store)
      assert :ok == Goblin.MemTable.Store.inc_streamers(c.store)
      assert 1 == Goblin.MemTable.Store.get_streamers_count(c.store)
      assert :ok == Goblin.MemTable.Store.deinc_streamers(c.store)
      assert 0 == Goblin.MemTable.Store.get_streamers_count(c.store)

      spawn(fn ->
        assert 0 == Goblin.MemTable.Store.get_streamers_count(c.store)
        assert :ok == Goblin.MemTable.Store.inc_streamers(c.store)
        assert 1 == Goblin.MemTable.Store.get_streamers_count(c.store)
        assert :ok == Goblin.MemTable.Store.deinc_streamers(c.store)
        assert 0 == Goblin.MemTable.Store.get_streamers_count(c.store)
        send(parent, :done)
      end)

      assert_receive :done
    end

    test "raises if missing streamers counters ref", c do
      :ets.delete(c.store, :streamers_counter_ref)

      assert_raise(RuntimeError, fn ->
        Goblin.MemTable.Store.get_streamers_count(c.store)
      end)

      assert_raise(RuntimeError, fn ->
        Goblin.MemTable.Store.inc_streamers(c.store)
      end)

      assert_raise(RuntimeError, fn ->
        Goblin.MemTable.Store.deinc_streamers(c.store)
      end)
    end
  end

  describe "size/1" do
    test "increases for inserted entries", c do
      size1 = Goblin.MemTable.Store.size(c.store)
      Goblin.MemTable.Store.insert(c.store, :key, 0, :val)
      size2 = Goblin.MemTable.Store.size(c.store)
      assert size2 > size1
    end
  end

  describe "wait_until_ready/2" do
    test "raises if not ready within timeout", c do
      assert_raise RuntimeError, fn ->
        Goblin.MemTable.Store.wait_until_ready(c.store, 200)
      end
    end

    test "returns :ok if ready", c do
      assert_raise RuntimeError, fn ->
        Goblin.MemTable.Store.wait_until_ready(c.store, 200)
      end

      assert :ok == Goblin.MemTable.Store.set_ready(c.store)
      assert :ok == Goblin.MemTable.Store.wait_until_ready(c.store, 200)
    end
  end
end
