defmodule SeaGoat.BloomFilterTest do
  use ExUnit.Case, async: true
  alias SeaGoat.BloomFilter

  test "new/2 returns valid Bloom Filter" do
    keys = Enum.to_list(1..100)
    size = 100
    assert %BloomFilter{} = bloom_filter = BloomFilter.new(keys, size)

    for n <- 1..100 do
      assert BloomFilter.is_member(bloom_filter, n)
    end

    refute BloomFilter.is_member(bloom_filter, size + 1)
  end
end
