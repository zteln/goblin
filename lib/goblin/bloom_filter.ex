defmodule Goblin.BloomFilter do
  @moduledoc false
  @default_false_positive_probability 0.05

  @derive {Inspect, except: [:array]}
  defstruct [
    :hashes,
    :array,
    :no_of_bits,
    :no_of_hashes,
    set: MapSet.new()
  ]

  @type t :: %__MODULE__{}
  @type no_of_bits :: non_neg_integer()
  @type no_of_hashes :: non_neg_integer()

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec put(t(), term()) :: t()
  def put(bf, key), do: %{bf | set: MapSet.put(bf.set, key)}

  @spec generate(t()) :: t()
  def generate(bf) do
    size = MapSet.size(bf.set)

    for key <- MapSet.to_list(bf.set), reduce: init(size) do
      acc ->
        update(acc, key)
    end
  end

  @spec to_tuple(t()) :: {:array.array(), no_of_bits(), no_of_hashes()}
  def to_tuple(bloom_filter) do
    %{
      array: array,
      no_of_bits: no_of_bits,
      no_of_hashes: no_of_hashes
    } = bloom_filter

    {array, no_of_bits, no_of_hashes}
  end

  @spec from_tuple({:array.array(), no_of_bits(), no_of_hashes()}) :: t()
  def from_tuple({array, no_of_bits, no_of_hashes}) do
    hashes = hashes(no_of_hashes, no_of_bits)

    %__MODULE__{
      array: array,
      no_of_bits: no_of_bits,
      no_of_hashes: no_of_hashes,
      hashes: hashes
    }
  end

  @spec is_member(t(), term()) :: boolean()
  def is_member(bloom_filter, key) do
    Enum.all?(bloom_filter.hashes, fn hash ->
      1 == :array.get(hash.(key), bloom_filter.array)
    end)
  end

  defp init(size) do
    no_of_bits = no_of_bits(size)
    no_of_hashes = no_of_hashes(size, no_of_bits)
    hashes = hashes(no_of_hashes, no_of_bits)
    array = :array.new(no_of_bits, default: 0)
    %__MODULE__{hashes: hashes, array: array, no_of_hashes: no_of_hashes, no_of_bits: no_of_bits}
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

  defp hashes(salt, range, hashes \\ [])
  defp hashes(0, _range, hashes), do: hashes

  defp hashes(salt, range, hashes) do
    hash = &:erlang.phash2({&1, salt}, range)
    hashes(salt - 1, range, [hash | hashes])
  end
end
