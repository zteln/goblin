defmodule Goblin.ReaderTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Reader

  @moduletag :tmp_dir

  setup_db(key_limit: 10, level_limit: 512)

  describe "get/4" do
    test "returns :not_found for non-existing key" do
      assert :not_found == Reader.get(:k, __MODULE__.Writer, __MODULE__.Store)
    end

    test "returns value corresponding to key", c do
      Goblin.put(c.db, :k, :v)

      assert {0, :v} == Reader.get(:k, __MODULE__.Writer, __MODULE__.Store)

      for n <- 1..10 do
        Goblin.put(c.db, :"k#{n}", :"v#{n}")
      end

      for n <- 1..10 do
        assert {n, :"v#{n}"} == Reader.get(:"k#{n}", __MODULE__.Writer, __MODULE__.Store)
      end
    end
  end

  describe "get_multi/3" do
    test "returns existing key-value pairs from provided keys", c do
      max = 15

      for n <- 1..max//2 do
        Goblin.put(c.db, :"k#{n}", :"v#{n}")
      end

      assert for(n <- 1..max//2, do: {:"k#{n}", div(n, 2), :"v#{n}"}) |> List.keysort(0) ==
               Reader.get_multi(
                 for(n <- 1..max, do: :"k#{n}"),
                 __MODULE__.Writer,
                 __MODULE__.Store
               )
               |> List.keysort(0)
    end
  end

  describe "select/4" do
    test "returns stream over all key-value pairs in sorted order", c do
      data =
        for n <- 1..15 do
          {:"k#{n}", :"v#{n}"}
        end

      Goblin.put_multi(c.db, data)

      assert List.keysort(data, 0) ==
               Reader.select(nil, nil, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()
    end

    test "returns stream over subset of key-value pairs", c do
      data =
        for n <- 1..15 do
          {n, :"v#{n}"}
        end

      Goblin.put_multi(c.db, data)

      assert List.keysort(Enum.take(data, 7), 0) ==
               Reader.select(nil, 7, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()

      assert List.keysort(Enum.take(data, -7), 0) ==
               Reader.select(9, nil, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()

      assert List.keysort(Enum.slice(data, 2, 9), 0) ==
               Reader.select(3, 11, __MODULE__.Writer, __MODULE__.Store) |> Enum.to_list()
    end
  end
end
