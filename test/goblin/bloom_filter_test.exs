defmodule Goblin.BloomFilterTest do
  use ExUnit.Case, async: true
  alias Goblin.BloomFilter

  test "Bloom filter can handle any term" do
    keys =
      StreamData.term()
      |> Enum.take(100)

    bloom_filter =
      for key <- keys, reduce: BloomFilter.new(fpp: 0.01, bit_array_size: 100) do
        acc ->
          BloomFilter.put(acc, key)
      end

    assert %BloomFilter{} = bloom_filter

    for key <- keys do
      assert BloomFilter.member?(bloom_filter, key)
    end

    random_key = :random_key
    refute random_key in keys
    refute BloomFilter.member?(bloom_filter, random_key)
  end
end
