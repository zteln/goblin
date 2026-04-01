defmodule Goblin.MemTable.IteratorTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable.{Store, Iterator}

  defp new_store(entries) do
    store = Store.new()

    Enum.each(entries, fn {key, seq, val} ->
      Store.insert(store, key, seq, val)
    end)

    store
  end

  defp stream_all(store, max_seq) do
    Goblin.Iterator.linear_stream(fn ->
      %Iterator{store: store, max_seq: max_seq}
    end)
    |> Enum.to_list()
  end

  describe "Iterable protocol" do
    test "iterates through all entries in key order" do
      store =
        new_store([
          {:b, 0, "b0"},
          {:a, 1, "a1"},
          {:c, 2, "c2"},
          {:a, 3, "a3"}
        ])

      result = stream_all(store, 100)

      assert result == [
               {:a, 3, "a3"},
               {:a, 1, "a1"},
               {:b, 0, "b0"},
               {:c, 2, "c2"}
             ]
    end

    test "filters entries at or above max_seq" do
      store =
        new_store([
          {:a, 0, "v0"},
          {:b, 1, "v1"},
          {:c, 2, "v2"},
          {:d, 3, "v3"}
        ])

      result = stream_all(store, 2)

      assert result == [
               {:a, 0, "v0"},
               {:b, 1, "v1"}
             ]
    end

    test "returns :ok for empty store" do
      store = Store.new()

      iterator =
        %Iterator{store: store, max_seq: 100}
        |> Goblin.Iterable.init()

      assert :ok = Goblin.Iterable.next(iterator)
    end

    test "init/1 returns the iterator unchanged" do
      store = Store.new()
      iterator = %Iterator{store: store, max_seq: 10}

      assert ^iterator = Goblin.Iterable.init(iterator)
    end

    test "deinit/1 returns :ok" do
      store = Store.new()
      iterator = %Iterator{store: store, max_seq: 10}

      assert :ok = Goblin.Iterable.deinit(iterator)
    end
  end
end
