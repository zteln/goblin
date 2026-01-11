defmodule Goblin.BloomFilter.BitArray do
  @moduledoc false

  @derive {Inspect, except: [:array]}
  defstruct [
    :size,
    :hash_params,
    :array
  ]

  @type t :: %__MODULE__{}

  @spec new(non_neg_integer(), number()) :: t()
  def new(size, fpp) do
    no_bits = no_bits(size, fpp)
    no_hashes = no_hashes(size, no_bits)
    hash_params = hash_params(no_hashes, no_bits)
    array = :array.new(no_bits, default: 0)
    %__MODULE__{size: size, hash_params: hash_params, array: array}
  end

  @spec add_key(t(), Goblin.db_key()) :: {:ok, t()} | {:error, :full}
  def add_key(%{size: size}, _key) when size <= 0, do: {:error, :full}

  def add_key(bit_array, key) do
    array =
      Enum.reduce(bit_array.hash_params, bit_array.array, fn hash_param, acc ->
        hash = hash(hash_param, key)
        :array.set(hash, 1, acc)
      end)

    {:ok, %{bit_array | array: array, size: bit_array.size - 1}}
  end

  @spec member?(t(), Goblin.db_key()) :: boolean()
  def member?(bit_array, key) do
    Enum.all?(bit_array.hash_params, fn hash_param ->
      hash = hash(hash_param, key)
      1 == :array.get(hash, bit_array.array)
    end)
  end

  defp no_bits(size, fpp) do
    floor(-size * :math.log(fpp) / :math.pow(:math.log(2), 2))
  end

  defp no_hashes(size, no_bits) do
    round(no_bits / size * :math.log(2))
  end

  defp hash_params(salt, range, params \\ [])
  defp hash_params(0, _range, params), do: params

  defp hash_params(salt, range, params) do
    hash_params(salt - 1, range, [{salt, range} | params])
  end

  defp hash({salt, range}, key) do
    :erlang.phash2({key, salt}, range)
  end
end

