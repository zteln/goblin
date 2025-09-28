defmodule SeaGoat.SSTables.SSTable do
  @moduledoc """
  An SSTable (Sorted String Table) implementation for the SeaGoat database.

  ## File Format

  SSTable files follow a specific binary format structure from start to end:

  ```
  ┌─────────────────┬──────────────┬─────────────────┐
  │ DATA BLOCKS     │ SEPARATOR    │ FOOTER          │
  │ (n × 512 bytes) │ (16 bytes)   │ (variable size) │
  └─────────────────┴──────────────┴─────────────────┘
  ```

  ### Data Blocks

  Each data block is exactly a multiple of 512 bytes and contains:

  ```elixir
  <<
    block_id::binary-size(16),     # "SEAGOATDBBLOCK00"
    span::integer-16,              # Number of blocks this entry spans
    encoded_kv_pair::binary,       # Encoded {key, value} tuple
    padding::binary                # Zero padding to fill 512 bytes
  >>
  ```

  ### Footer Structure

  The footer appears at the end of the file in this order:

  1. **Separator** (16 bytes): `"SEAGOATDBSEP0000"`
  2. **Bloom Filter** (variable size): Encoded bloom filter binary
  3. **Range** (variable size): Encoded key range information
  4. **Metadata** (56 bytes): File metadata
  5. **Magic** (16 bytes): `"SEAGOATDBFILE000"`

  ### Metadata Format

  The metadata section contains file structure information:

  ```elixir
  <<
    level::integer-32,            # SSTable level in LSM tree
    bf_size::integer-64,          # Bloom filter size in bytes
    bf_pos::integer-64,           # Bloom filter position from file start
    range_size::integer-64,       # Range data size in bytes
    range_pos::integer-64,        # Range data position from file start
    amount_of_blocks::integer-64, # Total number of data blocks
    data_span::integer-64         # Total size of data section
  >>
  ```
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

  @doc """
  Returns true if the argument matches the magic constant for SSTables, false otherwise.
  """
  @spec is_ss_table(binary()) :: boolean()
  def is_ss_table(<<@magic>>), do: true
  def is_ss_table(_), do: false

  @doc """
  Decodes the block header and returns `{:ok, span}`, where `span` is the multiple of 512-bytes the block spans over.
  `{:error, :eod}` is returned if the separator ID is matched on instead.
  `{:error, :not_block_start}` is returned if the block header does not start with the block ID.
  """
  @spec block_span(binary()) :: {:ok, non_neg_integer()} | {:error, :eod | :not_block_start}
  def block_span(<<@block_id, span::integer-16>>), do: {:ok, span}
  def block_span(<<@separator, _rest::binary>>), do: {:error, :eod}
  def block_span(_), do: {:error, :not_block_start}

  @doc """
  Returns how many 512-bytes blocks are needed for `size`.
  """
  @spec span(non_neg_integer()) :: non_neg_integer()
  def span(size), do: div(size + @block_size - 1, @block_size)

  @doc """
  Returns the sizes of a block, magic number, metadata table, and block header.
  """
  @spec size(:block | :magic | :metadata | :block_header) :: non_neg_integer()
  def size(:block), do: @block_size
  def size(:magic), do: @magic_size
  def size(:metadata), do: @metadata_size
  def size(:block_header), do: @block_header_size

  @doc """
  Encodes `key` and `value` into a block which has a byte size of `n * 512`, `n` a positive integer. It prepends a block header.
  """
  @spec encode_block(term(), term()) :: binary()
  def encode_block(key, value) do
    encoded = encode({key, value})
    block_size = byte_size(<<@block_id::binary, 0::integer-16, encoded::binary>>)
    span = span(block_size)
    padding = span * @block_size - block_size
    <<@block_id::binary, span::integer-16, encoded::binary, 0::size(padding)-unit(8)>>
  end

  @doc """
  Encodes the footer part of the SSTable file.
  The footer stores the Bloom filter, key range, metadata, and finally the magic number.
  """
  @spec encode_footer(
          non_neg_integer(),
          SeaGoat.BloomFilter.t(),
          {term(), term()},
          non_neg_integer(),
          non_neg_integer()
        ) :: binary()
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

  @spec decode_block(binary()) :: {:ok, {term(), term()}} | {:error, :not_a_block}
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

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
