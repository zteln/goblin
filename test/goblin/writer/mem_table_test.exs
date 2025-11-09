defmodule Goblin.Writer.MemTableTest do
  use ExUnit.Case, async: true
  alias Goblin.Writer.MemTable

  setup do
    table = MemTable.new(__MODULE__)
    %{table: table}
  end

  describe "upsert/4" do
    test "inserts new key-value pair", c do
      assert [] == :ets.tab2list(c.table)
      assert true == MemTable.upsert(c.table, :k, 0, :v)
      assert [{:k, 0, :v}] == :ets.tab2list(c.table)
    end

    test "updates key-value pair", c do
      assert true == MemTable.upsert(c.table, :k, 0, :v)
      assert [{:k, 0, :v}] == :ets.tab2list(c.table)
      assert true == MemTable.upsert(c.table, :k, 0, :w)
      assert [{:k, 0, :w}] == :ets.tab2list(c.table)
    end
  end

  describe "delete/3" do
    test "updates existing value with :tombstone", c do
      assert true == MemTable.upsert(c.table, :k, 0, :v)
      assert [{:k, 0, :v}] == :ets.tab2list(c.table)
      assert true == MemTable.delete(c.table, :k, 1)
      assert [{:k, 1, :tombstone}] == :ets.tab2list(c.table)
    end

    test "inserts :tombstone value if no key-value exists", c do
      assert [] == :ets.tab2list(c.table)
      assert true == MemTable.delete(c.table, :k, 0)
      assert [{:k, 0, :tombstone}] == :ets.tab2list(c.table)
    end
  end

  describe "read/3" do
    test "reads nothing if no commit seq exists", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)

      assert :not_found == MemTable.read(c.table, :k1, nil)
      assert :not_found == MemTable.read(c.table, :k2, nil)
      assert :not_found == MemTable.read(c.table, :k3, nil)
    end

    test "reads from latest committed seq no if seq not provided", c do
      MemTable.upsert(c.table, :k, 0, :v)
      MemTable.put_commit_seq(c.table, 0)
      assert {:k, 0, :v} == MemTable.read(c.table, :k, nil)
    end

    test "reads from latest provided seq no", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)

      assert {:k1, 0, :v1} == MemTable.read(c.table, :k1, 2)
      assert {:k2, 1, :v2} == MemTable.read(c.table, :k2, 2)
      assert {:k3, 2, :v3} == MemTable.read(c.table, :k3, 2)

      assert :not_found == MemTable.read(c.table, :k1, -1)
      assert :not_found == MemTable.read(c.table, :k2, 0)
      assert :not_found == MemTable.read(c.table, :k3, 1)
    end
  end

  describe "clean_seq_range/2" do
    test "deletes entries below provided seq no", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      assert [{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}] == :ets.tab2list(c.table)

      assert 1 == MemTable.clean_seq_range(c.table, 0)
      assert [{:k2, 1, :v2}, {:k3, 2, :v3}] == :ets.tab2list(c.table)

      assert 2 == MemTable.clean_seq_range(c.table, 2)
      assert [] == :ets.tab2list(c.table)
    end
  end

  describe "get_range/3" do
    test "returns nothing if commit seq does not exist", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)

      assert [] == MemTable.get_range(c.table, :k1, :k3)
    end

    test "returns all keys", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      MemTable.upsert(c.table, :k4, 3, :v4)
      MemTable.put_commit_seq(c.table, 3)

      assert [{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}, {:k4, 3, :v4}] ==
               MemTable.get_range(c.table, nil, nil)
    end

    test "returns subset range over min and max keys (inclusive)", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      MemTable.upsert(c.table, :k4, 3, :v4)
      MemTable.put_commit_seq(c.table, 3)

      assert [{:k2, 1, :v2}, {:k3, 2, :v3}] == MemTable.get_range(c.table, :k2, :k3)
    end

    test "returns subset range from min and onwards (inclusive)", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      MemTable.upsert(c.table, :k4, 3, :v4)
      MemTable.put_commit_seq(c.table, 3)

      assert [{:k2, 1, :v2}, {:k3, 2, :v3}, {:k4, 3, :v4}] ==
               MemTable.get_range(c.table, :k2, nil)
    end

    test "returns subset range from smallest to max (inclusive)", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      MemTable.upsert(c.table, :k4, 3, :v4)
      MemTable.put_commit_seq(c.table, 3)

      assert [{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}] ==
               MemTable.get_range(c.table, nil, :k3)
    end
  end

  describe "get_seq_range/2" do
    test "returns list of keys inbetween smallest and provided seq no", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      MemTable.upsert(c.table, :k4, 3, :v4)

      assert [{:k1, 0, :v1}, {:k2, 1, :v2}] == MemTable.get_seq_range(c.table, 1)
      assert [{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}] == MemTable.get_seq_range(c.table, 2)

      assert [{:k1, 0, :v1}, {:k2, 1, :v2}, {:k3, 2, :v3}, {:k4, 3, :v4}] ==
               MemTable.get_seq_range(c.table, 3)
    end
  end
end
