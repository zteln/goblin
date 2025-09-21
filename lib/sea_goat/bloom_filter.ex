defmodule SeaGoat.BloomFilter do
  @moduledoc """
  Generate Bloom filters.
  """
  @default_false_positive_probability 0.05

  @derive {Inspect, except: [:array]}
  defstruct [
    :hashes,
    :array
  ]

  @type t :: %__MODULE__{}

  @spec new(keys :: [term()], size :: non_neg_integer()) :: t()
  def new(keys, size) do
    for key <- keys, reduce: init(size) do
      acc ->
        update(acc, key)
    end
  end

  @spec is_member(bloom_filter :: t(), key :: term()) :: boolean()
  def is_member(bloom_filter, key) do
    Enum.all?(bloom_filter.hashes, fn hash ->
      1 == :array.get(hash.(key), bloom_filter.array)
    end)
  end

  defp init(size) do
    no_of_bits = no_of_bits(size)
    no_of_hashes = no_of_hashes(size, no_of_bits)
    hashes = hashes(no_of_hashes, no_of_bits, no_of_bits, [])
    array = :array.new(no_of_bits, default: 0)
    %__MODULE__{hashes: hashes, array: array}
  end

  defp update(bloom_filter, key) do
    for hash <- bloom_filter.hashes, reduce: bloom_filter do
      acc ->
        array = :array.set(hash.(key), 1, acc.array)
        %{acc | array: array}
    end
  end

  defp no_of_bits(size) do
    floor(-size * :math.log(@default_false_positive_probability) / :math.pow(:math.log(2), 2))
  end

  defp no_of_hashes(size, no_of_bits) do
    round(no_of_bits / size * :math.log(2))
  end

  defp hashes(0, _no_of_bits, _range, hashes), do: hashes

  defp hashes(no_of_hashes, no_of_bits, range, hashes) do
    hash = &rem(:erlang.phash2(&1, range), no_of_bits)
    hashes(no_of_hashes - 1, no_of_bits, range + 10, [hash | hashes])
  end
end
