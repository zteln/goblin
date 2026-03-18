defmodule Goblin.MemTable.IteratorTest do
  use ExUnit.Case, async: true

  alias Goblin.MemTable

  defp new_mem_table(entries) do
    mem_table = MemTable.new()

    Enum.each(entries, fn {key, seq, val} ->
      MemTable.insert(mem_table, key, seq, val)
    end)

    mem_table
  end

  defp stream_all(mem_table, max_seq) do
    Goblin.Iterator.linear_stream(fn ->
      %MemTable.Iterator{mem_table: mem_table, max_seq: max_seq}
    end)
    |> Enum.to_list()
  end

  describe "Iterable protocol" do
    test "iterates through all entries in key order" do
      mem_table =
        new_mem_table([
          {:b, 0, "b0"},
          {:a, 1, "a1"},
          {:c, 2, "c2"},
          {:a, 3, "a3"}
        ])

      result = stream_all(mem_table, 100)

      assert result == [
               {:a, 3, "a3"},
               {:a, 1, "a1"},
               {:b, 0, "b0"},
               {:c, 2, "c2"}
             ]
    end

    test "filters entries at or above max_seq" do
      mem_table =
        new_mem_table([
          {:a, 0, "v0"},
          {:b, 1, "v1"},
          {:c, 2, "v2"},
          {:d, 3, "v3"}
        ])

      result = stream_all(mem_table, 2)

      assert result == [
               {:a, 0, "v0"},
               {:b, 1, "v1"}
             ]
    end

    test "returns :ok for empty mem table" do
      mem_table = MemTable.new()

      iterator =
        %MemTable.Iterator{mem_table: mem_table, max_seq: 100}
        |> Goblin.Iterable.init()

      assert :ok = Goblin.Iterable.next(iterator)
    end

    test "init/1 returns the iterator unchanged" do
      mem_table = MemTable.new()
      iterator = %MemTable.Iterator{mem_table: mem_table, max_seq: 10}

      assert ^iterator = Goblin.Iterable.init(iterator)
    end

    test "deinit/1 returns :ok" do
      mem_table = MemTable.new()
      iterator = %MemTable.Iterator{mem_table: mem_table, max_seq: 10}

      assert :ok = Goblin.Iterable.deinit(iterator)
    end
  end
end
