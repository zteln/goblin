defmodule Goblin.BloomFilterTest do
  use ExUnit.Case, async: true
  alias Goblin.BloomFilter

  test "Bloom filter works as expected" do
    bloom_filter =
      for n <- 1..100, reduce: BloomFilter.new(fpp: 0.01, bit_array_size: 100) do
        acc ->
          BloomFilter.put(acc, n)
      end

    assert %BloomFilter{} = bloom_filter

    for n <- 1..100 do
      assert BloomFilter.member?(bloom_filter, n)
    end

    refute BloomFilter.member?(bloom_filter, 101)
  end
end
