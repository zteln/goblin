defmodule Goblin.DiskTables.Encoder do
  @moduledoc false
  @sst_block_id "GOBLINBLOCK00000"
  @sst_block_unit_size 512
  @sst_header_size byte_size(<<@sst_block_id::binary, 0::integer-16>>)

  @separator "GOBLINSEP0000000"
  @separator_size byte_size(@separator)

  @metadata_block_size byte_size(<<
                         0::integer-8,
                         0::integer-64,
                         0::integer-64,
                         0::integer-64,
                         0::integer-64,
                         0::integer-64,
                         0::integer-64,
                         0::integer-64,
                         0::integer-8
                       >>)

  @crc_block_size byte_size(<<0::integer-32>>)
  @size_block_size byte_size(<<0::integer-64>>)

  @magic "GOBLINFILE000000"
  @magic_size byte_size(@magic)

  @spec encode_sst_block(Goblin.triple(), boolean()) :: {binary(), pos_integer()}
  def encode_sst_block(triple, compress?) do
    data = encode(triple, compress?)
    data_size = byte_size(<<@sst_block_id::binary, 0::integer-16, data::binary>>)
    no_blocks = no_blocks(data_size)
    padding = no_blocks * @sst_block_unit_size - data_size

    sst_block =
      <<@sst_block_id::binary, no_blocks::integer-16, data::binary, 0::size(padding)-unit(8)>>

    {sst_block, no_blocks}
  end

  @spec decode_sst_header_block(binary()) ::
          {:ok, pos_integer()} | {:error, :end_of_sst | :invalid_sst_header}
  def decode_sst_header_block(<<@sst_block_id, no_blocks::integer-16>>),
    do: {:ok, no_blocks}

  def decode_sst_header_block(<<@separator, _rest::binary>>), do: {:error, :end_of_sst}
  def decode_sst_header_block(_), do: {:error, :invalid_sst_header}

  @spec decode_sst_block(binary()) :: {:ok, Goblin.triple()} | {:error, :invalid_sst_block}
  def decode_sst_block(<<@sst_block_id, _no_blocks::integer-16, data::binary>>),
    do: {:ok, decode(data)}

  def decode_sst_block(_), do: {:error, :invalid_sst_block}

  @spec encode_footer_block(
          Goblin.level_key(),
          Goblin.BloomFilter.t(),
          {Goblin.db_key(), Goblin.db_key()},
          {Goblin.seq_no(), Goblin.seq_no()},
          non_neg_integer(),
          pos_integer(),
          non_neg_integer(),
          non_neg_integer(),
          boolean()
        ) :: {binary(), non_neg_integer(), non_neg_integer()}
  def encode_footer_block(
        level_key,
        bloom_filter,
        key_range,
        seq_range,
        offset,
        no_blocks,
        crc,
        size,
        compress?
      ) do
    bf_block = encode({:bloom_filter, bloom_filter}, compress?)
    bf_block_pos = offset + @separator_size
    bf_block_size = byte_size(bf_block)

    key_range_block = encode({:key_range, key_range}, compress?)
    key_range_block_pos = bf_block_pos + bf_block_size
    key_range_block_size = byte_size(key_range_block)

    seq_range_block = encode({:seq_range, seq_range}, compress?)
    seq_range_block_pos = key_range_block_pos + key_range_block_size
    seq_range_block_size = byte_size(seq_range_block)

    compressed? = if compress?, do: 1, else: 0

    metadata_block = <<
      level_key::integer-8,
      bf_block_pos::integer-64,
      bf_block_size::integer-64,
      key_range_block_pos::integer-64,
      key_range_block_size::integer-64,
      seq_range_block_pos::integer-64,
      seq_range_block_size::integer-64,
      no_blocks::integer-64,
      compressed?::integer-8
    >>

    blocks = <<
      @separator::binary,
      bf_block::binary,
      key_range_block::binary,
      seq_range_block::binary,
      metadata_block::binary
    >>

    crc = update_crc(crc, blocks)
    size = size + byte_size(blocks)

    footer_block = <<
      blocks::binary,
      crc::integer-32,
      size::integer-64,
      @magic::binary
    >>

    {footer_block, size, crc}
  end

  @spec decode_metadata_block(binary()) ::
          {:ok,
           {Goblin.level_key(), {pos_integer(), non_neg_integer()},
            {pos_integer(), non_neg_integer()}, {pos_integer(), non_neg_integer()}, pos_integer(),
            boolean()}}
          | {:error, :invalid_metadata_block}
  def decode_metadata_block(<<
        level_key::integer-8,
        bf_block_pos::integer-64,
        bf_block_size::integer-64,
        key_range_block_pos::integer-64,
        key_range_block_size::integer-64,
        seq_range_block_pos::integer-64,
        seq_range_block_size::integer-64,
        no_blocks::integer-64,
        compressed?::integer-8
      >>) do
    {:ok,
     {
       level_key,
       {bf_block_pos, bf_block_size},
       {key_range_block_pos, key_range_block_size},
       {seq_range_block_pos, seq_range_block_size},
       no_blocks,
       compressed? == 1
     }}
  end

  def decode_metadata_block(_), do: {:error, :invalid_metadata_block}

  @spec decode_bloom_filter_block(binary()) ::
          {:ok, Goblin.BloomFilter.t()} | {:error, :invalid_bloom_filter_block}
  def decode_bloom_filter_block(bloom_filter_block) do
    case decode(bloom_filter_block) do
      {:bloom_filter, bloom_filter} -> {:ok, bloom_filter}
      _ -> {:error, :invalid_bloom_filter_block}
    end
  end

  @spec decode_key_range_block(binary()) ::
          {:ok, {Goblin.db_key(), Goblin.db_key()}} | {:error, :invalid_key_range_block}
  def decode_key_range_block(key_range_block) do
    case decode(key_range_block) do
      {:key_range, key_range} -> {:ok, key_range}
      _ -> {:error, :invalid_key_range_block}
    end
  end

  @spec decode_seq_range_block(binary()) ::
          {:ok, {Goblin.seq_no(), Goblin.seq_no()}} | {:error, :invalid_seq_range_block}
  def decode_seq_range_block(seq_range_block) do
    case decode(seq_range_block) do
      {:seq_range, seq_range} -> {:ok, seq_range}
      _ -> {:error, :invalid_seq_range_block}
    end
  end

  @spec decode_crc_block(binary()) :: {:ok, non_neg_integer()} | {:error, :invalid_crc_block}
  def decode_crc_block(<<crc::integer-32>>), do: {:ok, crc}
  def decode_crc_block(_), do: {:error, :invalid_crc_block}

  @spec decode_size_block(binary()) :: {:ok, non_neg_integer()} | {:error, :invalid_crc_block}
  def decode_size_block(<<size::integer-64>>), do: {:ok, size}
  def decode_size_block(_), do: {:error, :invalid_size_block}

  @spec sst_block_unit_size() :: non_neg_integer()
  def sst_block_unit_size, do: @sst_block_unit_size

  @spec sst_header_size() :: non_neg_integer()
  def sst_header_size, do: @sst_header_size

  @spec metadata_block_size() :: non_neg_integer()
  def metadata_block_size, do: @metadata_block_size

  @spec crc_block_size() :: non_neg_integer()
  def crc_block_size, do: @crc_block_size

  @spec size_block_size() :: non_neg_integer()
  def size_block_size, do: @size_block_size

  @spec magic_size() :: non_neg_integer()
  def magic_size, do: @magic_size

  @spec validate_magic_block(binary()) :: :ok | {:error, :invalid_magic}
  def validate_magic_block(@magic), do: :ok
  def validate_magic_block(_), do: {:error, :invalid_magic}

  @spec update_crc(non_neg_integer(), binary()) :: non_neg_integer()
  def update_crc(crc, bin), do: :erlang.crc32(crc, bin)

  defp encode(term, true), do: :erlang.term_to_binary(term, [:compressed])
  defp encode(term, false), do: :erlang.term_to_binary(term)
  defp decode(data), do: :erlang.binary_to_term(data)
  defp no_blocks(size), do: div(size + @sst_block_unit_size - 1, @sst_block_unit_size)
end
