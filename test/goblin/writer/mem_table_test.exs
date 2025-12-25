defmodule Goblin.Writer.MemTableTest do
  use ExUnit.Case, async: true
  alias Goblin.Writer.MemTable

  setup do
    table = MemTable.new(__MODULE__)
    %{table: table}
  end

  describe "upsert/4" do
    test "inserts new key-value pair", c do
      assert true == MemTable.upsert(c.table, :k, 0, :v)
      MemTable.put_commit_seq(c.table, 1)
      assert {:k, 0, :v} == MemTable.read(c.table, :k, nil)
    end

    test "updates key-value pair", c do
      assert true == MemTable.upsert(c.table, :k, 0, :v)
      MemTable.put_commit_seq(c.table, 1)
      assert {:k, 0, :v} == MemTable.read(c.table, :k, nil)
      assert true == MemTable.upsert(c.table, :k, 1, :w)
      MemTable.put_commit_seq(c.table, 2)
      assert {:k, 1, :w} == MemTable.read(c.table, :k, nil)
    end
  end

  describe "delete/3" do
    test "updates existing value with :$goblin_tombstone", c do
      assert true == MemTable.upsert(c.table, :k, 0, :v)
      MemTable.put_commit_seq(c.table, 1)
      assert {:k, 0, :v} == MemTable.read(c.table, :k, nil)
      assert true == MemTable.delete(c.table, :k, 1)
      MemTable.put_commit_seq(c.table, 2)
      assert {:k, 1, :"$goblin_tombstone"} == MemTable.read(c.table, :k, nil)
    end

    test "inserts :$goblin_tombstone value if no key-value exists", c do
      assert true == MemTable.delete(c.table, :k, 0)
      MemTable.put_commit_seq(c.table, 1)
      assert {:k, 0, :"$goblin_tombstone"} == MemTable.read(c.table, :k, nil)
    end
  end

  describe "read/3" do
    test "reads up to (exclusive) of provided sequence number", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)

      assert :not_found == MemTable.read(c.table, :k1, 0)
      assert :not_found == MemTable.read(c.table, :k2, 0)
      assert :not_found == MemTable.read(c.table, :k3, 0)

      assert {:k1, 0, :v1} == MemTable.read(c.table, :k1, 1)
      assert :not_found == MemTable.read(c.table, :k2, 1)
      assert :not_found == MemTable.read(c.table, :k3, 1)

      assert {:k1, 0, :v1} == MemTable.read(c.table, :k1, 2)
      assert {:k2, 1, :v2} == MemTable.read(c.table, :k2, 2)
      assert :not_found == MemTable.read(c.table, :k3, 2)

      assert {:k1, 0, :v1} == MemTable.read(c.table, :k1, 3)
      assert {:k2, 1, :v2} == MemTable.read(c.table, :k2, 3)
      assert {:k3, 2, :v3} == MemTable.read(c.table, :k3, 3)
    end
  end

  describe "clean_seq_range/2" do
    test "deletes entries below provided seq no", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)
      MemTable.put_commit_seq(c.table, 3)

      assert {:k1, 0, :v1} == MemTable.read(c.table, :k1, nil)
      assert {:k2, 1, :v2} == MemTable.read(c.table, :k2, nil)
      assert {:k3, 2, :v3} == MemTable.read(c.table, :k3, nil)

      assert 1 == MemTable.clean_seq_range(c.table, 0)

      assert :not_found == MemTable.read(c.table, :k1, nil)
      assert {:k2, 1, :v2} == MemTable.read(c.table, :k2, nil)
      assert {:k3, 2, :v3} == MemTable.read(c.table, :k3, nil)

      assert 2 == MemTable.clean_seq_range(c.table, 2)

      assert :not_found == MemTable.read(c.table, :k1, nil)
      assert :not_found == MemTable.read(c.table, :k2, nil)
      assert :not_found == MemTable.read(c.table, :k3, nil)
    end
  end

  describe "iterator/2" do
    test "is iterable", c do
      MemTable.upsert(c.table, :k1, 0, :v1)
      MemTable.upsert(c.table, :k2, 1, :v2)
      MemTable.upsert(c.table, :k3, 2, :v3)

      assert %MemTable.Iterator{} = iterator = MemTable.iterator(c.table, 3)
      assert ^iterator = Goblin.Iterable.init(iterator)

      assert {{:k1, 0, :v1}, iterator} = Goblin.Iterable.next(iterator)
      assert {{:k2, 1, :v2}, iterator} = Goblin.Iterable.next(iterator)
      assert {{:k3, 2, :v3}, iterator} = Goblin.Iterable.next(iterator)
      assert :ok == Goblin.Iterable.next(iterator)
      assert :ok == Goblin.Iterable.close(iterator)
    end

    test "iterates up to provided sequence number", c do
      for n <- 1..10 do
        MemTable.upsert(c.table, n, n - 1, "v-#{n}")
        MemTable.put_commit_seq(c.table, n)
      end

      assert %MemTable.Iterator{max_seq: 8} = iterator = MemTable.iterator(c.table, 8)

      iterator =
        for n <- 1..8, reduce: iterator do
          iterator ->
            key = n
            seq = n - 1
            value = "v-#{n}"
            assert {{^key, ^seq, ^value}, iterator} = Goblin.Iterable.next(iterator)
            iterator
        end

      assert :ok == Goblin.Iterable.next(iterator)
      assert :ok == Goblin.Iterable.close(iterator)
    end
  end

  describe "wait_until_memtable_ready/2" do
    test "returns :ok if MemTable is ready", c do
      MemTable.set_ready(c.table)
      assert :ok == MemTable.wait_until_memtable_ready(c.table)
    end

    test "raises if not ready within timeout", c do
      assert_raise RuntimeError, fn ->
        MemTable.wait_until_memtable_ready(c.table, 200)
      end
    end
  end
end
