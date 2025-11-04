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

  test "Bloom filter can be generated with different false positive probabilities" do
    bloom_filter1 =
      for n <- 1..100, reduce: BloomFilter.new() do
        acc ->
          BloomFilter.put(acc, n)
      end
      |> BloomFilter.generate(0.05)

    bloom_filter2 =
      for n <- 1..100, reduce: BloomFilter.new() do
        acc ->
          BloomFilter.put(acc, n)
      end
      |> BloomFilter.generate(0.01)

    assert length(bloom_filter2.hashes) > length(bloom_filter1.hashes)
    assert :array.size(bloom_filter2.array) > :array.size(bloom_filter1.array)
  end
end
