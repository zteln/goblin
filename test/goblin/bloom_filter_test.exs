defmodule Goblin.BloomFilterTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Goblin.BloomFilter

  @probe_count 100_000

  defp build(key_count, fpp) do
    keys = Enum.map(1..key_count, &{:member, &1})
    {keys, BloomFilter.new(key_count, keys, fpp)}
  end

  defp measured_fpp(bf) do
    Enum.count(1..@probe_count, &BloomFilter.member?(bf, {:not_member, &1})) / @probe_count
  end

  # Textbook optimum: bits per key = -ln(p) / ln(2)^2
  defp optimal_bits_per_key(fpp), do: -:math.log(fpp) / :math.pow(:math.log(2), 2)

  describe "member?/2" do
    test "distinguishes structurally similar terms" do
      keys = [{:a, 1}, [:a, 1], "a1", :a1, 1.0, {1}]
      bf = BloomFilter.new(length(keys), keys, 0.0001)

      assert Enum.all?(keys, &BloomFilter.member?(bf, &1))
      refute BloomFilter.member?(bf, {:a, 2})
      refute BloomFilter.member?(bf, [:a, 2])
      refute BloomFilter.member?(bf, "a2")
    end
  end

  describe "new/3" do
    test "the bit array is exactly large enough for no_bits" do
      for {n, fpp} <- [{1, 0.5}, {7, 0.1}, {100, 0.01}, {5_000, 0.001}] do
        {_, bf} = build(n, fpp)
        assert byte_size(bf.bits) == div(bf.no_bits + 7, 8)
        assert bf.no_hashes >= 1
      end
    end

    test "is deterministic" do
      {_, bf1} = build(500, 0.01)
      {_, bf2} = build(500, 0.01)
      assert bf1 == bf2
    end

    test "an empty filter rejects everything" do
      bf = BloomFilter.new(0, [], 0.01)
      assert bf.no_bits >= 8 and bf.no_hashes >= 1
      refute BloomFilter.member?(bf, :anything)
      refute Enum.any?(1..100, &BloomFilter.member?(bf, &1))
    end

    test "allocates at least 8 bits and 1 hash for tiny filters" do
      # 1 key at fpp 0.5 would need ~1.4 bits and ~1 hash without the floor
      bf = BloomFilter.new(1, [:only], 0.5)
      assert bf.no_bits >= 8
      assert bf.no_hashes >= 1
      assert BloomFilter.member?(bf, :only)

      # 1000 keys at fpp 0.9 would round to 0 hashes without the floor
      {keys, bf} = build(1_000, 0.9)
      assert bf.no_hashes == 1
      assert Enum.all?(keys, &BloomFilter.member?(bf, &1))
    end
  end

  describe "false positive probability" do
    test "holds across the accepted range of fpp values" do
      for fpp <- [0.5, 0.1, 0.05, 0.01, 0.001, 0.0001] do
        {keys, bf} = build(2_000, fpp)

        assert Enum.all?(keys, &BloomFilter.member?(bf, &1)),
               "false negative at fpp=#{fpp}"

        # 2x headroom, plus an absolute floor so a handful of hits out of
        # 100k probes cannot fail the tightest targets
        measured = measured_fpp(bf)
        bound = max(2 * fpp, 0.0005)
        assert measured <= bound, "fpp=#{fpp}: measured #{measured} > #{bound}"
        assert measured >= fpp / 4
      end
    end

    test "allocates bits per key close to the textbook optimum" do
      for fpp <- [0.1, 0.01, 0.001, 0.0001], size <- [100, 1_000, 10_000] do
        {_, bf} = build(size, fpp)
        assert_in_delta bf.no_bits / size, optimal_bits_per_key(fpp), 0.01
      end
    end
  end

  @tag :property_tests
  property "no false negatives for any fpp in (0, 1)" do
    check all(
            terms <- list_of(term(), min_length: 1),
            fpp <- float(min: 0.0001, max: 0.9999)
          ) do
      bf = BloomFilter.new(Enum.count(terms), terms, fpp)
      assert Enum.all?(terms, &BloomFilter.member?(bf, &1))
    end
  end
end
