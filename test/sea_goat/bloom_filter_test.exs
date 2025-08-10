defmodule SeaGoat.BloomFilterTest do
  use ExUnit.Case, async: true
  alias SeaGoat.BloomFilter

  describe "new/2" do
    test "returns new Bloom filter" do
      keys = 0..100 |> Enum.to_list()

      no_of_bits =
        floor(
          -length(keys) * :math.log(0.05) /
            :math.pow(:math.log(2), 2)
        )

      no_of_hashes = ceil(div(no_of_bits, length(keys)) * :math.log(2))

      assert %BloomFilter{} = bloom_filter = BloomFilter.new(keys, length(keys))

      assert no_of_bits >= MapSet.size(bloom_filter.set)
      assert no_of_hashes == length(bloom_filter.hashes)
    end
  end

  describe "is_member/2" do
    setup do
      keys = 0..200//2 |> Enum.to_list()
      bloom_filter = BloomFilter.new(keys, length(keys))
      %{bloom_filter: bloom_filter}
    end

    test "returns true positive", c do
      for n <- 0..200//2 do
        assert BloomFilter.is_member(c.bloom_filter, n)
      end
    end

    test "returns true negative", c do
      refute BloomFilter.is_member(c.bloom_filter, 1)
    end

    test "returns false positive", c do
      assert BloomFilter.is_member(c.bloom_filter, 109)
    end
  end
end
