defmodule Goblin.DiskTable.SST do
  @moduledoc false
  alias Goblin.BloomFilter

  defstruct [
    :file,
    :level_key,
    bloom_filter: BloomFilter.new(),
    key_range: {nil, nil},
    seq_range: {nil, 0},
    size: 0,
    no_blocks: 0,
    crc: :erlang.crc32(<<>>)
  ]

  @type t :: %__MODULE__{}
  @type size :: non_neg_integer()
  @type position :: non_neg_integer()
  @type offset :: non_neg_integer()
  @type no_blocks :: non_neg_integer()

  @magic "GOBLINFILE000000"
  @magic_size byte_size(@magic)

  @separator "GOBLINSEP0000000"
  @separator_size byte_size(@separator)

  @metadata_size byte_size(<<
                   0::integer-32,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-32
                 >>)

  @block_id "GOBLINBLOCK00000"
  @block_size 512

  @block_header_size byte_size(<<@block_id::binary, 0::integer-16>>)

  def new(file, level_key) do
    %__MODULE__{file: file, level_key: level_key}
  end

  def add_data(sst, {key, seq, _value} = triple, compress?) do
    data = encode(triple, compress?)
    data_size = byte_size(<<@block_id::binary, 0::integer-16, data::binary>>)
    span = span(data_size)
    padding = span * @block_size - data_size
    block = <<@block_id::binary, span::integer-16, data::binary, 0::size(padding)-unit(8)>>

    sst =
      %{sst | no_blocks: sst.no_blocks + span, size: sst.size + byte_size(block)}
      |> update_key_range(key)
      |> update_seq_range(seq)
      |> update_bloom_filter(key)
      |> update_crc(block)

    {block, sst}
  end

  def add_metadata(sst, offset, bf_fpp, compress?) do
    bloom_filter = BloomFilter.generate(sst.bloom_filter, bf_fpp)
    enc_bf = encode({:bloom_filter, bloom_filter}, compress?)
    bf_pos = offset + @separator_size
    bf_size = byte_size(enc_bf)
    enc_key_range = encode({:key_range, sst.key_range}, compress?)
    key_range_pos = bf_pos + bf_size
    key_range_size = byte_size(enc_key_range)
    enc_seq_range = encode({:seq_range, sst.seq_range}, compress?)
    seq_range_pos = key_range_pos + key_range_size
    seq_range_size = byte_size(enc_seq_range)

    size =
      sst.size +
        bf_size +
        key_range_size +
        seq_range_size +
        @metadata_size +
        @separator_size +
        @magic_size

    metadata =
      <<
        sst.level_key::integer-32,
        bf_pos::integer-64,
        bf_size::integer-64,
        key_range_pos::integer-64,
        key_range_size::integer-64,
        seq_range_pos::integer-64,
        seq_range_size::integer-64,
        sst.no_blocks::integer-64,
        size::integer-64,
        offset::integer-64,
        sst.crc::integer-32
      >>

    metadata_block = <<
      @separator::binary,
      enc_bf::binary,
      enc_key_range::binary,
      enc_seq_range::binary,
      metadata::binary,
      @magic::binary
    >>

    {metadata_block, %{sst | size: size, bloom_filter: bloom_filter}}
  end

  defp update_key_range(%{key_range: {nil, nil}} = sst, key) do
    %{sst | key_range: {key, key}}
  end

  defp update_key_range(sst, key) do
    %{key_range: {min_key, _}} = sst
    %{sst | key_range: {min_key, key}}
  end

  defp update_seq_range(%{seq_range: {nil, _}} = sst, seq) do
    %{sst | seq_range: {seq, seq}}
  end

  defp update_seq_range(sst, seq) do
    %{seq_range: {min_seq, _}} = sst
    %{sst | seq_range: {min_seq, seq}}
  end

  defp update_bloom_filter(sst, key) do
    %{sst | bloom_filter: BloomFilter.put(sst.bloom_filter, key)}
  end

  defp update_crc(sst, block) do
    %{sst | crc: :erlang.crc32(sst.crc, block)}
  end

  @spec is_ss_table(binary()) :: boolean()
  def is_ss_table(<<@magic>>), do: true
  def is_ss_table(_), do: false

  @spec block_span(binary()) :: {:ok, non_neg_integer()} | {:error, :eod | :not_block_start}
  def block_span(<<@block_id, span::integer-16>>), do: {:ok, span}
  def block_span(<<@separator, _rest::binary>>), do: {:error, :eod}
  def block_span(_), do: {:error, :not_block_start}

  @spec span(non_neg_integer()) :: non_neg_integer()
  def span(size), do: div(size + @block_size - 1, @block_size)

  @spec size(:block | :magic | :metadata | :block_header) :: non_neg_integer()
  def size(:block), do: @block_size
  def size(:magic), do: @magic_size
  def size(:separator), do: @separator_size
  def size(:metadata), do: @metadata_size
  def size(:block_header), do: @block_header_size

  @spec decode_block(binary()) :: {:ok, Goblin.triple()} | {:error, :invalid_block}
  def decode_block(<<@block_id, _span::integer-16, triple::binary>>) do
    {:ok, decode(triple)}
  end

  def decode_block(_), do: {:error, :invalid_block}

  @spec decode_metadata(binary()) ::
          {:ok,
           {Goblin.db_level_key(), size(), position(), size(), position(), size(), position(),
            no_blocks(), size(), offset(), non_neg_integer()}}
          | {:error, :invalid_metadata}
  def decode_metadata(<<
        level_key::integer-32,
        bf_pos::integer-64,
        bf_size::integer-64,
        key_range_pos::integer-64,
        key_range_size::integer-64,
        seq_range_pos::integer-64,
        seq_range_size::integer-64,
        no_blocks::integer-64,
        size::integer-64,
        offset::integer-64,
        crc::integer-32
      >>) do
    {:ok,
     {
       level_key,
       bf_pos,
       bf_size,
       key_range_pos,
       key_range_size,
       seq_range_pos,
       seq_range_size,
       no_blocks,
       size,
       offset,
       crc
     }}
  end

  def decode_metadata(_), do: {:error, :invalid_metadata}

  @spec decode_bloom_filter(binary()) ::
          {:ok, BloomFilter.t()} | {:error, :invalid_bloom_filter}
  def decode_bloom_filter(encoded) do
    case decode(encoded) do
      {:bloom_filter, %BloomFilter{} = bloom_filter} -> {:ok, bloom_filter}
      _ -> {:error, :invalid_bloom_filter}
    end
  end

  @spec decode_key_range(binary()) ::
          {:ok, {Goblin.db_key(), Goblin.db_key()}} | {:error, :invalid_range}
  def decode_key_range(encoded) do
    case decode(encoded) do
      {:key_range, {_, _} = key_range} -> {:ok, key_range}
      _ -> {:error, :invalid_key_range}
    end
  end

  @spec decode_seq_range(binary()) ::
          {:ok, {Goblin.seq_no(), Goblin.seq_no()}} | {:error, :invalid_seq_range}
  def decode_seq_range(encoded) do
    case decode(encoded) do
      {:seq_range, seq_range} -> {:ok, seq_range}
      _ -> {:error, :invalid_seq_range}
    end
  end

  defp encode(term, true), do: :erlang.term_to_binary(term, [:compressed])
  defp encode(term, false), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
