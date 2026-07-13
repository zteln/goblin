defmodule GoblinBloomFilterTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Goblin.BloomFilter

  test "member?/2 returns false (maybe) if not a member" do
    keys = Enum.to_list(1..200)
    bf = BloomFilter.new(length(keys), keys, 0.01)
    refute BloomFilter.member?(bf, 201)
  end

  test "false positive rate stays within target" do
    fpp = 0.01
    key_count = 1_000
    keys = Enum.map(1..key_count, &{:member, &1})
    bf = BloomFilter.new(key_count, keys, fpp)

    # No false negatives
    for n <- 1..key_count do
      assert BloomFilter.member?(bf, {:member, n})
    end

    probe_count = 100_000

    false_positives =
      Enum.count(1..probe_count, fn n ->
        BloomFilter.member?(bf, {:not_member, n})
      end)

    measured_fpp = false_positives / probe_count 
    assert measured_fpp <= fpp * 2
  end

  @tag :property_tests
  property "each term put in the Bloom filter is a member" do
    fpp = 0.01

    check all(terms <- list_of(term(), min_length: 1)) do
      bf = BloomFilter.new(Enum.count(terms), terms, fpp)
      assert Enum.all?(terms, &BloomFilter.member?(bf, &1))
    end
  end
end
