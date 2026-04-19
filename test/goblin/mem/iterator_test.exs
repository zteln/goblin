defmodule Goblin.Mem.IteratorTest do
  use ExUnit.Case, async: true

  alias Goblin.Mem.{Iterator, Table}

  defp new_table(entries) do
    table = Table.new()

    Enum.each(entries, fn {key, seq, val} ->
      Table.insert(table, key, seq, val)
    end)

    table
  end

  defp stream_all(table, max_seq) do
    Goblin.Iterator.linear_stream(fn ->
      %Iterator{table: table, max_seq: max_seq}
    end)
    |> Enum.to_list()
  end

  describe "Iterable protocol" do
    test "iterates through all entries in key order" do
      table =
        new_table([
          {:b, 0, "b0"},
          {:a, 1, "a1"},
          {:c, 2, "c2"},
          {:a, 3, "a3"}
        ])

      result = stream_all(table, 100)

      assert result == [
               {:a, 3, "a3"},
               {:a, 1, "a1"},
               {:b, 0, "b0"},
               {:c, 2, "c2"}
             ]
    end

    test "filters entries at or above max_seq" do
      table =
        new_table([
          {:a, 0, "v0"},
          {:b, 1, "v1"},
          {:c, 2, "v2"},
          {:d, 3, "v3"}
        ])

      result = stream_all(table, 2)

      assert result == [
               {:a, 0, "v0"},
               {:b, 1, "v1"}
             ]
    end

    test "returns :ok for empty table" do
      table = Table.new()

      iterator =
        %Iterator{table: table, max_seq: 100}
        |> Goblin.Iterable.init()

      assert :ok = Goblin.Iterable.next(iterator)
    end

    test "init/1 returns the iterator unchanged" do
      table = Table.new()
      iterator = %Iterator{table: table, max_seq: 10}

      assert ^iterator = Goblin.Iterable.init(iterator)
    end

    test "deinit/1 returns :ok" do
      table = Table.new()
      iterator = %Iterator{table: table, max_seq: 10}

      assert :ok = Goblin.Iterable.deinit(iterator)
    end
  end
end
