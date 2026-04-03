defmodule Goblin.BloomFilter do
  @moduledoc false
  alias Goblin.BloomFilter.BitArray

  defstruct [
    :fpp,
    :bit_array_size,
    :bit_arrays
  ]

  @default_bit_array_size 10_000
  @default_fpp 0.01
  @tightening_ratio 0.5
  @type t :: %__MODULE__{}

  @spec new(keyword()) :: t()
  def new(opts) do
    fpp = opts[:fpp] || @default_fpp
    segment_fpp = calculate_fpp([], fpp)
    bit_array_size = opts[:bit_array_size] || @default_bit_array_size
    bit_array = BitArray.new(bit_array_size, segment_fpp)
    %__MODULE__{bit_arrays: [bit_array], fpp: fpp, bit_array_size: bit_array_size}
  end

  @spec put(t(), Goblin.db_key()) :: t()
  def put(bf, key) do
    [current_bit_array | bit_arrays] = bf.bit_arrays

    case BitArray.add_key(current_bit_array, key) do
      {:ok, bit_array} ->
        %{bf | bit_arrays: [bit_array | bit_arrays]}

      {:error, :full} ->
        segment_fpp = calculate_fpp(bf.bit_arrays, bf.fpp)
        bit_array = BitArray.new(bf.bit_array_size, segment_fpp)

        %{bf | bit_arrays: [bit_array | bf.bit_arrays]}
        |> put(key)
    end
  end

  @spec member?(t(), term()) :: boolean()
  def member?(bf, key) do
    Enum.any?(bf.bit_arrays, &BitArray.member?(&1, key))
  end

  defp calculate_fpp(bit_arrays, fpp) do
    fpp * (1 - @tightening_ratio) * :math.pow(@tightening_ratio, length(bit_arrays))
  end
end
