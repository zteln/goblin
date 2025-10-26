defmodule Goblin.BloomFilterTest do
  use ExUnit.Case, async: true
  alias Goblin.BloomFilter

  test "Bloom filter works as expected" do
    bloom_filter =
      for n <- 1..100, reduce: BloomFilter.new() do
        acc ->
          BloomFilter.put(acc, n)
      end
      |> BloomFilter.generate()

    assert %BloomFilter{} = bloom_filter

    for n <- 1..100 do
      assert BloomFilter.is_member(bloom_filter, n)
    end

    refute BloomFilter.is_member(bloom_filter, 101)
  end
end
