defmodule Goblin.BloomFilter do
  @moduledoc false

  @derive {Inspect, except: [:array]}
  defstruct [
    :hashes,
    :array,
    set: MapSet.new()
  ]

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec put(t(), term()) :: t()
  def put(bf, key), do: %{bf | set: MapSet.put(bf.set, key)}

  @spec generate(t(), number()) :: t()
  def generate(bf, fpp) do
    size = MapSet.size(bf.set)

    for key <- MapSet.to_list(bf.set), reduce: init(size, fpp) do
      acc ->
        update(acc, key)
    end
  end

  @spec is_member(t(), term()) :: boolean()
  def is_member(bf, key) do
    Enum.all?(bf.hashes, fn hash ->
      hash = calc_hash(hash, key)
      1 == :array.get(hash, bf.array)
    end)
  end

  defp init(size, fpp) do
    no_of_bits = no_of_bits(size, fpp)
    no_of_hashes = no_of_hashes(size, no_of_bits)
    hashes = hashes(no_of_hashes, no_of_bits)
    array = :array.new(no_of_bits, default: 0)
    %__MODULE__{hashes: hashes, array: array}
  end

  defp update(bf, key) do
    for hash <- bf.hashes, reduce: bf do
      acc ->
        hash = calc_hash(hash, key)
        array = :array.set(hash, 1, acc.array)
        %{acc | array: array}
    end
  end

  defp no_of_bits(size, fpp) do
    floor(-size * :math.log(fpp) / :math.pow(:math.log(2), 2))
  end

  defp no_of_hashes(size, no_of_bits) do
    round(no_of_bits / size * :math.log(2))
  end

  defp hashes(salt, range, hashes \\ [])
  defp hashes(0, _range, hashes), do: hashes

  defp hashes(salt, range, hashes) do
    hash = {:erlang, :phash2, [salt, range]}
    hashes(salt - 1, range, [hash | hashes])
  end

  defp calc_hash({m, f, [salt, range]}, key) do
    apply(m, f, [{key, salt}, range])
  end
end
