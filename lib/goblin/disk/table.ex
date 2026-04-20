defmodule Goblin.Disk.Table do
  @moduledoc false

  alias Goblin.BloomFilter

  @table_block_size 1024

  defstruct [
    :path,
    :level_key,
    :bloom_filter,
    :key_range,
    :seq_range,
    size: 0,
    no_blocks: 0
  ]

  @type t :: %__MODULE__{
          path: Path.t(),
          level_key: non_neg_integer(),
          bloom_filter: BloomFilter.t(),
          key_range: {term(), term()},
          seq_range: {non_neg_integer(), non_neg_integer()},
          size: non_neg_integer(),
          no_blocks: non_neg_integer()
        }

  @spec new(Path.t(), non_neg_integer(), keyword()) :: t()
  def new(path, level_key, bf_opts) do
    bloom_filter =
      BloomFilter.new(bit_array_size: bf_opts[:bf_bit_array_size], fpp: bf_opts[:bf_fpp])

    %__MODULE__{
      path: path,
      level_key: level_key,
      bloom_filter: bloom_filter
    }
  end

  @spec add_to_table(t(), Goblin.triple(), non_neg_integer()) :: t()
  def add_to_table(table, triple, size) do
    {key, seq, _val} = triple

    key_range =
      case table.key_range do
        nil -> {key, key}
        {min, _} -> {min, key}
      end

    seq_range =
      case table.seq_range do
        nil -> {seq, seq}
        {min, _} -> {min, seq}
      end

    bloom_filter = BloomFilter.put(table.bloom_filter, key)
    no_blocks = table.no_blocks + div(size, @table_block_size)

    %{
      table
      | key_range: key_range,
        seq_range: seq_range,
        bloom_filter: bloom_filter,
        no_blocks: no_blocks,
        size: table.size + size
    }
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(table, key),
    do: within_min_max?(table, key) and bloom_filter_member?(table, key)

  @spec block_size() :: non_neg_integer()
  def block_size, do: @table_block_size

  @spec within_bounds?(t(), term(), term()) :: boolean()
  def within_bounds?(_table, nil, nil), do: true
  def within_bounds?(%{key_range: {_, max}}, min, nil), do: min <= max
  def within_bounds?(%{key_range: {min, _}}, nil, max), do: min <= max

  def within_bounds?(%{key_range: {min1, max1}}, min2, max2),
    do: min2 <= max1 and min1 <= max2

  defp within_min_max?(%{key_range: {min, max}}, key) when min <= key and key <= max,
    do: true

  defp within_min_max?(_table, _key), do: false

  defp bloom_filter_member?(table, key) do
    BloomFilter.member?(table.bloom_filter, key)
  end
end
