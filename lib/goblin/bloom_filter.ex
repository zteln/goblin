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
  @type t :: %__MODULE__{}

  @spec new(keyword()) :: t()
  def new(opts) do
    fpp = opts[:fpp] || @default_fpp
    bit_array_size = opts[:bit_array_size] || @default_bit_array_size
    bit_array = BitArray.new(bit_array_size, fpp)
    %__MODULE__{bit_arrays: [bit_array], fpp: fpp, bit_array_size: bit_array_size}
  end

  @spec put(t(), Goblin.db_key()) :: t()
  def put(bf, key) do
    [current_bit_array | bit_arrays] = bf.bit_arrays

    case BitArray.add_key(current_bit_array, key) do
      {:ok, bit_array} ->
        %{bf | bit_arrays: [bit_array | bit_arrays]}

      {:error, :full} ->
        bit_array = BitArray.new(bf.bit_array_size, bf.fpp)

        %{bf | bit_arrays: [bit_array | bf.bit_arrays]}
        |> put(key)
    end
  end

  @spec member?(t(), term()) :: boolean()
  def member?(bf, key) do
    Enum.any?(bf.bit_arrays, &BitArray.member?(&1, key))
  end
end
