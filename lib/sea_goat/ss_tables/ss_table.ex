defmodule SeaGoat.SSTables.SSTable do
  @moduledoc """
  An SSTable is of the following format:

  DATA::n * 512bytes
  SEPARATOR::16bytes
  BLOOM FILTER::binary
  RANGE::binary
  METADATA::n
  MAGIC::16bytes

  where DATA is
  [{id::16bytes, span::u16, {key, value}::binary}]
  and METADATA is
  <<
    level::u32, 
    bf_size::u64, 
    bf_pos::u64, 
    range_size::u64, 
    range_pos::u64, 
    amount_of_blocks::u64,
    data_span::u64
  >>
  """
  @magic "SEAGOATDBFILE000"
  @magic_size byte_size(@magic)

  @metadata_size byte_size(<<
                   0::integer-32,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64,
                   0::integer-64
                 >>)

  @separator "SEAGOATDBSEP0000"
  @block_id "SEAGOATDBBLOCK00"
  @block_size 512

  @block_header_size byte_size(<<@block_id::binary, 0::integer-16>>)

  def new, do: <<>>

  def is_ss_table(<<@magic>>), do: true
  def is_ss_table(_), do: false

  def block_span(<<@block_id, span::integer-16>>), do: {:ok, span}
  def block_span(<<@separator, _rest::binary>>), do: {:error, :eod}
  def block_span(_), do: {:error, :not_block_start}

  def size(:block), do: @block_size
  def size(:magic), do: @magic_size
  def size(:metadata), do: @metadata_size
  def size(:block_header), do: @block_header_size

  def encode_block(key, value) do
    encoded = encode({key, value})
    block_size = byte_size(<<@block_id::binary, 0::integer-16, encoded::binary>>)
    span = span(block_size, @block_size)
    padding = span * @block_size - block_size
    <<@block_id::binary, span::integer-16, encoded::binary, 0::size(padding)-unit(8)>>
  end

  def encode_footer(level, bloom_filter, range, offset, no_of_blocks) do
    encoded_bf = encode({:bloom_filter, bloom_filter})
    bf_size = byte_size(encoded_bf)
    bf_pos = offset + byte_size(@separator)
    encoded_range = encode({:range, range})
    range_size = byte_size(encoded_range)
    range_pos = bf_pos + bf_size

    metadata =
      <<
        level::integer-32,
        bf_size::integer-64,
        bf_pos::integer-64,
        range_size::integer-64,
        range_pos::integer-64,
        no_of_blocks::integer-64,
        offset::integer-64
      >>

    <<
      @separator::binary,
      encoded_bf::binary,
      encoded_range::binary,
      metadata::binary,
      @magic::binary
    >>
  end

  def decode_block(<<@block_id, _span::integer-16, pair::binary>>) do
    {:ok, decode(pair)}
  end

  def decode_block(_), do: {:error, :not_a_block}

  def decode_metadata(<<
        level::integer-32,
        bf_size::integer-64,
        bf_pos::integer-64,
        range_size::integer-64,
        range_pos::integer-64,
        no_of_blocks::integer-64,
        offset::integer-64
      >>) do
    {:ok, {level, bf_size, bf_pos, range_size, range_pos, no_of_blocks, offset}}
  end

  def decode_metadata(_), do: {:error, :invalid_metadata}

  def decode_bloom_filter(encoded) do
    case decode(encoded) do
      {:bloom_filter, bloom_filter} -> {:ok, bloom_filter}
      _ -> {:error, :invalid_bloom_filter}
    end
  end

  def decode_range(encoded) do
    case decode(encoded) do
      {:range, range} -> {:ok, range}
      _ -> {:error, :invalid_range}
    end
  end

  defp span(size, unit), do: div(size + unit - 1, unit)
  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
