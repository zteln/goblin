defmodule Goblin.BloomFilter.BitArrayTest do
  use ExUnit.Case, async: true

  alias Goblin.BloomFilter.BitArray

  test "can update bit_array and check membership" do
    bit_array = BitArray.new(100, 0.01)

    refute BitArray.member?(bit_array, :key)
    assert {:ok, bit_array} = BitArray.add_key(bit_array, :key)
    assert BitArray.member?(bit_array, :key)
  end

  test "can only add a finite amount of keys" do
    bit_array = BitArray.new(100, 0.01)

    bit_array =
      for n <- 1..100, reduce: bit_array do
        bit_array ->
          assert {:ok, bit_array} = BitArray.add_key(bit_array, n)
          bit_array
      end

    assert {:error, :full} == BitArray.add_key(bit_array, :key)
  end

  test "false positive rate stays within target" do
    size = 10_000
    fpp = 0.01

    bit_array = BitArray.new(size, fpp)

    bit_array =
      for n <- 1..size, reduce: bit_array do
        bit_array ->
          {:ok, bit_array} = BitArray.add_key(bit_array, {:member, n})
          bit_array
      end

    # No false negatives
    for n <- 1..size do
      assert BitArray.member?(bit_array, {:member, n})
    end

    # Measure false positive rate with non-member keys
    probe_count = 100_000

    false_positives =
      Enum.count(1..probe_count, fn n ->
        BitArray.member?(bit_array, {:not_member, n})
      end)

    measured_fpp = false_positives / probe_count
    assert measured_fpp <= fpp * 2
  end

  test "binary size matches optimal formula" do
    for {size, fpp} <- [{100, 0.01}, {10_000, 0.01}, {10_000, 0.001}] do
      bit_array = BitArray.new(size, fpp)
      expected_bits = floor(-size * :math.log(fpp) / :math.pow(:math.log(2), 2))
      expected_bytes = div(expected_bits + 7, 8)

      assert byte_size(bit_array.bits) == expected_bytes
    end
  end
end
