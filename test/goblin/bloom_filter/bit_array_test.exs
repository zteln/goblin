defmodule Goblin.BloomFilter.BitArrayTest do
  use ExUnit.Case, async: true

  test "can update bit_array and check membershit" do
    bit_array = Goblin.BloomFilter.BitArray.new(100, 0.01)

    refute Goblin.BloomFilter.BitArray.member?(bit_array, :key)
    assert {:ok, bit_array} = Goblin.BloomFilter.BitArray.add_key(bit_array, :key)
    assert Goblin.BloomFilter.BitArray.member?(bit_array, :key)
  end

  test "can only add a finite amount of keys" do
    bit_array = Goblin.BloomFilter.BitArray.new(100, 0.01)

    bit_array =
      for n <- 1..100, reduce: bit_array do
        bit_array ->
          assert {:ok, bit_array} = Goblin.BloomFilter.BitArray.add_key(bit_array, n)
          bit_array
      end

    assert {:error, :full} == Goblin.BloomFilter.BitArray.add_key(bit_array, :key)
  end
end

