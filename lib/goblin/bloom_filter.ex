defmodule Goblin.BloomFilter do
  @moduledoc false

  defstruct [
    :bits,
    :size,
    :no_bits,
    :no_hashes
  ]

  @type t :: %__MODULE__{
          bits: <<>>,
          size: non_neg_integer(),
          no_bits: non_neg_integer(),
          no_hashes: non_neg_integer()
        }

  @spec new(non_neg_integer(), list(term()), number()) :: t()
  def new(no_keys, keys, fpp) do
    no_bits = no_bits(no_keys, fpp)
    no_bytes = div(no_bits + 7, 8)
    no_hashes = no_hashes(no_keys, no_bits)

    bits =
      keys
      |> into_positions(no_hashes, no_bits)
      |> build_filter(no_bytes)

    %__MODULE__{
      bits: bits,
      size: no_keys,
      no_bits: no_bits,
      no_hashes: no_hashes
    }
  end

  @spec member?(t(), term()) :: boolean()
  def member?(bf, key) do
    hashes(key, bf.no_hashes, bf.no_bits)
    |> Enum.all?(fn hash ->
      byte_pos = div(hash, 8)
      bit_offset = rem(hash, 8)
      byte = :binary.at(bf.bits, byte_pos)
      Bitwise.band(byte, Bitwise.bsl(1, bit_offset)) != 0
    end)
  end

  defp into_positions(keys, no_hashes, no_bits) do
    keys
    |> Enum.flat_map(&hashes(&1, no_hashes, no_bits))
    |> Enum.sort()
  end

  defp build_filter(positions, next_byte \\ 0, no_bytes, acc \\ [])

  defp build_filter([], next_byte, no_bytes, acc),
    do: :erlang.iolist_to_binary([acc | :binary.copy(<<0>>, no_bytes - next_byte)])

  defp build_filter([pos | _] = positions, next_byte, no_bytes, acc) do
    byte_idx = div(pos, 8)
    gap = :binary.copy(<<0>>, byte_idx - next_byte)
    {byte, rest} = fill_byte(positions, byte_idx, 0)
    build_filter(rest, byte_idx + 1, no_bytes, [acc, gap, byte])
  end

  defp fill_byte([pos | rest], byte_idx, byte) when div(pos, 8) == byte_idx do
    fill_byte(rest, byte_idx, Bitwise.bor(byte, Bitwise.bsl(1, rem(pos, 8))))
  end

  defp fill_byte(positions, _byte_idx, byte), do: {byte, positions}

  defp hashes(key, no_hashes, range) do
    h1 = :erlang.phash2({key, 1}, range)
    h2 = :erlang.phash2({key, 2}, range)

    for i <- 0..(no_hashes - 1) do
      rem(h1 + i * h2, range)
    end
  end

  defp no_bits(size, fpp), do: floor(-size * :math.log(fpp) / :math.pow(:math.log(2), 2))
  defp no_hashes(size, no_bits), do: round(no_bits / size * :math.log(2))
end
