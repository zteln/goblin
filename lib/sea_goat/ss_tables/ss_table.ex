defmodule SeaGoat.SSTables.SSTable do
  @identifier 0xFABCDEF0
  @block_separator 0xFFFFFFFF
  @header_size byte_size(<<0::unsigned-integer-32>>)
  @kv_header_size byte_size(<<0::unsigned-integer-32>>)
  @footer_size byte_size(<<
                 0::unsigned-integer-8,
                 0::unsigned-integer-32,
                 0::unsigned-integer-32,
                 0::unsigned-integer-32
               >>)

  def make do
    <<@identifier::unsigned-integer-32>>
  end

  def offset_calc(offset, footer \\ nil, part)

  def offset_calc(offset, _, :footer), do: offset - @footer_size

  def offset_calc(offset, {_, range_size, _, _} = footer, :range),
    do: offset_calc(offset, footer, :index) - range_size

  def offset_calc(offset, {_, _, index_size, _} = footer, :index),
    do: offset_calc(offset, footer, :bloom_filter) - index_size

  def offset_calc(offset, {_, _, _, bloom_filter_size} = footer, :bloom_filter),
    do: offset_calc(offset, footer, :footer) - bloom_filter_size

  def footer_part({level, _, _, _}, :level), do: level
  def footer_part({_, range_size, _, _}, :range), do: range_size
  def footer_part({_, _, index_size, _}, :index), do: index_size
  def footer_part({_, _, _, bloom_filter_size}, :bloom_filter), do: bloom_filter_size

  def size(:header), do: @header_size
  def size(:footer), do: @footer_size
  def size(:kv_header), do: @kv_header_size

  def is_block(<<@identifier::unsigned-integer-32>>), do: true
  def is_block(_), do: false

  def encode(:meta, opts) do
    level = opts[:level]
    range = opts[:range]
    index = opts[:index]
    bloom_filter = opts[:bloom_filter]

    range = :erlang.term_to_binary({:range, range})
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

  def encode(:data, opts) do
    key = opts[:key]
    value = opts[:value]

    bin = :erlang.term_to_binary({key, value})
    size = byte_size(bin) 
    <<size::unsigned-integer-32, bin::binary>>
  end

  @spec decode(type :: atom(), encoded :: binary()) :: {:ok, term()} | {:error, :invalid_format}
  def decode(:footer, <<
        level::unsigned-integer-8,
        range_size::unsigned-integer-32,
        index_size::unsigned-integer-32,
        bloom_filter_size::unsigned-integer-32
      >>) do
    {:ok, {level, range_size, index_size, bloom_filter_size}}
  end

  def decode(:kv_header, <<@block_separator::unsigned-integer-32>>) do
    {:ok, :end_of_data}
  end

  def decode(:kv_header, <<size::unsigned-integer-32>>) do
    {:ok, size}
  end

  def decode(:data, <<_size::unsigned-integer-32, bin::binary>>) do
    case :erlang.binary_to_term(bin) do
      {_key, _value} = data -> {:ok, data}
      _ -> {:error, :invalid_format}
    end
  end

  def decode(:meta_part, meta_part) do
    case :erlang.binary_to_term(meta_part) do
      {:range, range} -> {:ok, range}
      {:index, index} -> {:ok, index}
      {:bloom_filter, bloom_filter} -> {:ok, bloom_filter}
      _ -> {:error, :invalid_format}
    end
  end

  def decode(_, _), do: {:error, :invalid_format}
end
