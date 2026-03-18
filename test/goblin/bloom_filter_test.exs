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

  test "false positive rate stays bounded across multiple segments" do
    fpp = 0.01
    bit_array_size = 100
    key_count = 1_000

    bloom_filter =
      for n <- 1..key_count, reduce: BloomFilter.new(fpp: fpp, bit_array_size: bit_array_size) do
        acc -> BloomFilter.put(acc, {:member, n})
      end

    # Verify scaling occurred
    assert length(bloom_filter.bit_arrays) > 1

    # No false negatives
    for n <- 1..key_count do
      assert BloomFilter.member?(bloom_filter, {:member, n})
    end

    # Combined FPP must stay within target despite multiple segments
    probe_count = 100_000

    false_positives =
      Enum.count(1..probe_count, fn n ->
        BloomFilter.member?(bloom_filter, {:not_member, n})
      end)

    measured_fpp = false_positives / probe_count
    assert measured_fpp <= fpp * 2
  end

  test "total binary size is compact and proportional to key count" do
    fpp = 0.01
    bit_array_size = 1_000
    key_count = 5_000

    bloom_filter =
      for n <- 1..key_count, reduce: BloomFilter.new(fpp: fpp, bit_array_size: bit_array_size) do
        acc -> BloomFilter.put(acc, n)
      end

    segment_count = length(bloom_filter.bit_arrays)
    assert segment_count == div(key_count, bit_array_size)

    total_size =
      bloom_filter.bit_arrays
      |> Enum.map(fn ba -> byte_size(ba.bits) end)
      |> Enum.sum()

    # Each segment is ~1,300-2,000 bytes for these parameters.
    # Total should be well under 10 KB for 5 segments.
    assert total_size < segment_count * 2_500
    assert total_size > segment_count * 1_000
  end
end
