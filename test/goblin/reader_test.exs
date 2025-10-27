defmodule Goblin.ReaderTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Reader

  @moduletag :tmp_dir
  @db_opts [
    id: :reader_test,
    name: :reader_test,
    key_limit: 10,
    sync_interval: 50,
    wal_name: :reader_test_wal,
    manifest_name: :reader_test_manifest
  ]

  setup_db(@db_opts)

  test "get/3/4 gets existing keys", c do
    assert :ok == Goblin.Writer.put(c.writer, 1, "v-1")
    assert {0, "v-1"} == Reader.get(1, c.writer, c.store)

    for n <- 1..10 do
      assert :ok == Goblin.Writer.put(c.writer, n, "v-#{n}")
    end

    for n <- 1..10 do
      assert {n, "v-#{n}"} == Reader.get(n, c.writer, c.store)
    end
  end

  test "get/3/4 returns :not_found for non-existing keys", c do
    assert :not_found == Reader.get(:non_existing_key, c.writer, c.store)
  end

  test "get_multi/3/4 returns list of sequences and values for keys", c do
    for n <- 1..5 do
      assert :ok == Goblin.Writer.put(c.writer, n, "v-#{n}")
    end

    assert for(n <- 5..1//-1, do: {n, n - 1, "v-#{n}"}) ==
             Goblin.Reader.get_multi(Enum.to_list(1..5), c.writer, c.store)

    for n <- 6..10 do
      assert :ok == Goblin.Writer.put(c.writer, n, "v-#{n}")
    end

    assert for(n <- 10..1//-1, do: {n, n - 1, "v-#{n}"}) ==
             Goblin.Reader.get_multi(Enum.to_list(1..10), c.writer, c.store)

    assert for(n <- 5..2//-1, do: {n, n - 1, "v-#{n}"}) ==
             Goblin.Reader.get_multi(Enum.to_list(2..5), c.writer, c.store)
  end

  test "get_multi/3/4 returns :not_found for non-existing keys", c do
    for n <- 1..5 do
      assert :ok == Goblin.Writer.put(c.writer, n, "v-#{n}")
    end

    assert for(_n <- 10..6//-1, do: :not_found) ==
             Goblin.Reader.get_multi(Enum.to_list(6..10), c.writer, c.store)

    for n <- 6..10 do
      assert :ok == Goblin.Writer.put(c.writer, n, "v-#{n}")
    end

    assert [
             {10, 9, "v-10"},
             {9, 8, "v-9"},
             {8, 7, "v-8"},
             :not_found
           ] == Goblin.Reader.get_multi(Enum.to_list(8..11), c.writer, c.store)
  end
end
