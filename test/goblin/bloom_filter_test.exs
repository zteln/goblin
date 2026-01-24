defmodule Goblin.BloomFilterTest do
  use ExUnit.Case, async: true
  alias Goblin.BloomFilter

  test "Bloom filter can handle any term" do
    keys =
      StreamData.term()
      |> Enum.take(200)

    bloom_filter =
      for key <- keys, reduce: BloomFilter.new(fpp: 0.01, bit_array_size: 100) do
        acc ->
          BloomFilter.put(acc, key)
      end

    assert %BloomFilter{} = bloom_filter

    for key <- keys do
      assert BloomFilter.member?(bloom_filter, key)
    end
  end

  test "member?/2 returns false (maybe) if not a member" do
    bloom_filter =
      for key <- 1..200, reduce: BloomFilter.new(fpp: 0.01, bit_array_size: 100) do
        acc ->
          BloomFilter.put(acc, key)
      end

    refute BloomFilter.member?(bloom_filter, 201)
  end
end
