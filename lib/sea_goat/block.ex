defmodule SeaGoat.Block do
  @moduledoc """
  Responsible for encoding and decoding data and meta information in block files.
  A block file contains two parts: data and meta, joined together by a 32-bit uint separator token.


  BLOCK_FILE = DATA + SEP + META
  DATA = [KV_HEADER + KV]
  KV_HEADER = 32-bit uint
  KV = {key, value}
  SEP = <<0xFFFFFFFF::unsigned-integer-32>>
  META = RANGE + INDEX + BLOOM_FILTER + META_FOOTER
  RANGE = {:range, {smallest, largest}}
  INDEX = {:index, %{}}
  BLOOM_FILTER = {:bloom_filter, %{}}
  META_FOOTER = level 8-bit uint + range_size 32-bit uint + index_size 32-bit uint + bloom_filter_size 32-bit uint
  """
  @block_separator 0xFFFFFFFF
  @kv_header_size byte_size(<<0::unsigned-integer-32>>)
  @footer_size byte_size(<<
                 0::unsigned-integer-8,
                 0::unsigned-integer-32,
                 0::unsigned-integer-32,
                 0::unsigned-integer-32
               >>)

  def offset_calc(offset, footer \\ nil, part)

  def offset_calc(offset, _, :footer), do: offset - @footer_size

  def offset_calc(offset, {_, range_size, _, _} = footer, :range),
    do: offset_calc(offset, footer, :index) - range_size

  def offset_calc(offset, {_, _, index_size, _} = footer, :index),
    do: offset_calc(offset, footer, :bloom_filter) - index_size

  def offset_calc(offset, {_, _, _, bloom_filter_size} = footer, :bloom_filter),
    do: offset_calc(offset, footer, :footer) - bloom_filter_size

  def join(data, meta) do
    <<data::binary, @block_separator::unsigned-integer-32, meta::binary>>
  end

  def encode_block(key, value) do
    bin = :erlang.term_to_binary({key, value})
    size = byte_size(bin)
    <<size::unsigned-integer-32, bin::binary>>
  end

  def encode_meta(level, smallest, largest, index, bloom_filter) do
    range = :erlang.term_to_binary({:range, {smallest, largest}})
    index = :erlang.term_to_binary({:index, index})
    bloom_filter = :erlang.term_to_binary({:bloom_filter, bloom_filter})

    range_size = byte_size(range)
    index_size = byte_size(index)
    bloom_filter_size = byte_size(bloom_filter)

    <<
      @block_separator::unsigned-integer-32,
      range::binary,
      index::binary,
      bloom_filter::binary,
      level::unsigned-integer-8,
      range_size::unsigned-integer-32,
      index_size::unsigned-integer-32,
      bloom_filter_size::unsigned-integer-32
    >>
  end

  def decode_metafooter(<<
        level::unsigned-integer-8,
        range_size::unsigned-integer-32,
        index_size::unsigned-integer-32,
        bloom_filter_size::unsigned-integer-32
      >>) do
    {:ok, {level, range_size, index_size, bloom_filter_size}}
  end

  def decode_metafooter(_), do: {:error, :invalid_format}

  def decode_kv_header(<<@block_separator::unsigned-integer-32>>) do
    {:ok, :end_of_data}
  end

  def decode_kv_header(<<size::unsigned-integer-32>>) do
    {:ok, size}
  end

  def decode_kv_header(_block), do: {:error, :invalid_format}

  def decode_block(<<_size::unsigned-integer-32, bin::binary>>) do
    case :erlang.binary_to_term(bin) do
      {_key, _value} = decoded -> {:ok, decoded}
      _ -> {:error, :unable_to_decode}
    end
  end

  def decode_block(_block), do: {:error, :invalid_format}

  def metafooter_size, do: @footer_size
  def kv_header_size, do: @kv_header_size

  def decode_metadata(part) do
    case :erlang.binary_to_term(part) do
      {:range, range} -> {:ok, range}
      {:index, index} -> {:ok, index}
      {:bloom_filter, bloom_filter} -> {:ok, bloom_filter}
      _ -> {:error, :unable_to_decode}
    end
  end
end
