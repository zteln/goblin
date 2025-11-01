defmodule Goblin.ReaderTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Reader

  @moduletag :tmp_dir
  @db_opts [
    id: :reader_test,
    name: __MODULE__,
    key_limit: 10,
    sync_interval: 50,
    wal_name: :reader_test_wal,
    manifest_name: :reader_test_manifest
  ]

  setup_db(@db_opts)

  test "get/3/4 gets existing keys", c do
    assert :ok == Goblin.Writer.put(c.registry, 1, "v-1")
    assert {0, "v-1"} == Reader.get(1, c.registry)

    for n <- 1..10 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    for n <- 1..10 do
      assert {n, "v-#{n}"} == Reader.get(n, c.registry)
    end
  end

  test "get/3/4 returns :not_found for non-existing keys", c do
    assert :not_found == Reader.get(:non_existing_key, c.registry)
  end

  test "get_multi/3/4 returns list of sequences and values for keys", c do
    for n <- 1..5 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    assert for(n <- 5..1//-1, do: {n - 1, n, "v-#{n}"}) ==
             Goblin.Reader.get_multi(Enum.to_list(1..5), c.registry)

    for n <- 6..10 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    assert for(n <- 10..1//-1, do: {n - 1, n, "v-#{n}"}) ==
             Goblin.Reader.get_multi(Enum.to_list(1..10), c.registry)

    assert for(n <- 5..2//-1, do: {n - 1, n, "v-#{n}"}) ==
             Goblin.Reader.get_multi(Enum.to_list(2..5), c.registry)
  end

  test "get_multi/3/4 returns :not_found for non-existing keys", c do
    for n <- 1..5 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    assert [] == Goblin.Reader.get_multi(Enum.to_list(6..10), c.registry)

    for n <- 6..10 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    assert [
             {9, 10, "v-10"},
             {8, 9, "v-9"},
             {7, 8, "v-8"}
           ] == Goblin.Reader.get_multi(Enum.to_list(8..11), c.registry)
  end

  test "select/4 returns empty stream when empty db", c do
    assert [] == Reader.select(nil, nil, c.registry) |> Enum.to_list()
  end

  test "select/4 returns entire range of keys", c do
    for n <- 1..20 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    assert for(n <- 1..20, do: {n, "v-#{n}"}) ==
             Reader.select(nil, nil, c.registry) |> Enum.to_list()
  end

  test "select/4 returns partial range of keys (inclusive)", c do
    for n <- 1..20 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    assert for(n <- 5..10, do: {n, "v-#{n}"}) ==
             Reader.select(5, 10, c.registry) |> Enum.to_list()

    assert for(n <- 1..10, do: {n, "v-#{n}"}) ==
             Reader.select(-5, 10, c.registry) |> Enum.to_list()

    assert [] == Reader.select(-5, 0, c.registry) |> Enum.to_list()
    assert [] == Reader.select(21, 30, c.registry) |> Enum.to_list()
  end

  test "select/4 returns latest writes", c do
    for n <- 1..20 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    for n <- 5..10 do
      assert :ok == Goblin.Writer.put(c.registry, n, "w-#{n}")
    end

    for n <- 15..20 do
      assert :ok == Goblin.Writer.put(c.registry, n, "u-#{n}")
    end

    range =
      for n <- 1..20 do
        cond do
          n >= 5 and n <= 10 -> {n, "w-#{n}"}
          n >= 15 and n <= 20 -> {n, "u-#{n}"}
          true -> {n, "v-#{n}"}
        end
      end

    assert range == Reader.select(nil, nil, c.registry) |> Enum.to_list()
  end

  test "select/4 skips :tombstone entries", c do
    for n <- 1..20 do
      assert :ok == Goblin.Writer.put(c.registry, n, "v-#{n}")
    end

    for n <- 5..10 do
      assert :ok == Goblin.Writer.remove(c.registry, n)
    end

    assert for(n <- 1..4, do: {n, "v-#{n}"}) ++ for(n <- 11..20, do: {n, "v-#{n}"}) ==
             Reader.select(nil, nil, c.registry) |> Enum.to_list()

    for n <- 1..5 do
      assert :ok == Goblin.Writer.remove(c.registry, n)
    end

    assert for(n <- 11..20, do: {n, "v-#{n}"}) ==
             Reader.select(nil, nil, c.registry) |> Enum.to_list()
  end
end
