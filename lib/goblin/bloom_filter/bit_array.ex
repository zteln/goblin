defmodule Goblin.BloomFilter.BitArray do
  @moduledoc false

  defstruct [
    :size,
    :no_bits,
    :no_hashes,
    :bits
  ]

  @type t :: %__MODULE__{}

  @doc "Create a new bit array."
  @spec new(non_neg_integer(), number()) :: t()
  def new(size, fpp) do
    no_bits = no_bits(size, fpp)
    no_hashes = no_hashes(size, no_bits)
    bits = :binary.copy(<<0>>, div(no_bits + 7, 8))
    %__MODULE__{size: size, no_bits: no_bits, no_hashes: no_hashes, bits: bits}
  end

  @doc "Add key to bit array if not already full."
  @spec add_key(t(), Goblin.db_key()) :: {:ok, t()} | {:error, :full}
  def add_key(%{size: size}, _key) when size <= 0, do: {:error, :full}

  def add_key(bit_array, key) do
    bits =
      key
      |> hashes(bit_array.no_hashes, bit_array.no_bits)
      |> Enum.reduce(bit_array.bits, fn hash, acc ->
        byte_pos = div(hash, 8)
        bit_offset = rem(hash, 8)
        <<left::binary-size(byte_pos), byte::8, right::binary>> = acc
        <<left::binary, Bitwise.bor(byte, Bitwise.bsl(1, bit_offset))::8, right::binary>>
      end)

    {:ok, %{bit_array | bits: bits, size: bit_array.size - 1}}
  end

  @doc "Check if key is a member in the bit array."
  @spec member?(t(), Goblin.db_key()) :: boolean()
  def member?(bit_array, key) do
    %{bits: bits} = bit_array

    key
    |> hashes(bit_array.no_hashes, bit_array.no_bits)
    |> Enum.all?(fn hash ->
      byte_pos = div(hash, 8)
      bit_offset = rem(hash, 8)
      byte = :binary.at(bits, byte_pos)
      Bitwise.band(byte, Bitwise.bsl(1, bit_offset)) != 0
    end)
  end

  defp no_bits(size, fpp) do
    floor(-size * :math.log(fpp) / :math.pow(:math.log(2), 2))
  end

  defp hashes(key, no_hashes, range) do
    h1 = hash1(key, range)
    h2 = hash2(key, range)

    for i <- 0..(no_hashes - 1) do
      rem(h1 + i * h2, range)
    end
  end

  defp hash1(x, range), do: :erlang.phash2({x, 1}, range)
  defp hash2(x, range), do: :erlang.phash2({x, 2}, range)

  defp no_hashes(size, no_bits) do
    round(no_bits / size * :math.log(2))
  end
end
