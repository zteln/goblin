defmodule Talon.SSTs.SST do
  @moduledoc """
  An SST (Sorted String Table) implementation for the Talon database.

  ## File Format

  SST files follow a specific binary format structure from start to end:

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
    block_id::binary-size(16),     # "TALONBLOCK000000"
    span::integer-16,              # Number of blocks this entry spans
    data::binary,                  # Encoded {seq, key, value} tuple
    padding::binary                # Zero padding to fill 512 bytes
  >>
  ```

  ### Footer Structure

  The footer appears at the end of the file in this order:

  1. **Separator** (16 bytes): `"TALONSEP00000000"`
  2. **Bloom Filter** (variable size): Encoded bloom filter binary
  3. **Range** (variable size): Encoded key range information
  4. **Metadata** (56 bytes): File metadata
  5. **Magic** (16 bytes): `"TALONFILE0000000"`

  ### Metadata Format

  The metadata section contains file structure information:

  ```elixir
  <<
    level_key::integer-32,            # SST level in LSM tree
    bf_size::integer-64,          # Bloom filter size in bytes
    bf_pos::integer-64,           # Bloom filter position from file start
    key_range_size::integer-64,   # Range data size in bytes
    key_range_pos::integer-64,    # Range data position from file start
    priority_size::integer-64,    # Range data size in bytes
    priority_pos::integer-64,     # Range data position from file start
    no_of_blocks::integer-64,     # Total number of data blocks
    size::integer-64,             # Size of SST
    data_span::integer-64         # Total size of data section
  >>
  ```
  """

  alias Talon.BloomFilter

  @type range :: {term(), term()}
  @type level :: non_neg_integer()
  @type size :: non_neg_integer()
  @type position :: non_neg_integer()
  @type offset :: non_neg_integer()
  @type no_of_blocks :: non_neg_integer()
  @type triple :: {Talon.db_sequence(), Talon.db_key(), Talon.db_value()}

  @magic "TALONFILE0000000"
  @magic_size byte_size(@magic)

  @separator "TALONSEP00000000"
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
                   0::integer-64
                 >>)

  @block_id "TALONBLOCK000000"
  @block_size 512

  @block_header_size byte_size(<<@block_id::binary, 0::integer-16>>)

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

  # @spec encode_block(term(), term()) :: binary()
  def encode_block(seq, key, value) do
    encoded = encode({seq, key, value})
    block_size = byte_size(<<@block_id::binary, 0::integer-16, encoded::binary>>)
    span = span(block_size)
    padding = span * @block_size - block_size
    <<@block_id::binary, span::integer-16, encoded::binary, 0::size(padding)-unit(8)>>
  end

  # @spec encode_footer(
  #         level(),
  #         BloomFilter.t(),
  #         range(),
  #         non_neg_integer(),
  #         non_neg_integer(),
  #         offset(),
  #         no_of_blocks()
  #       ) :: binary()
  def encode_footer(
        level_key,
        %BloomFilter{} = bloom_filter,
        key_range,
        priority,
        offset,
        no_of_blocks,
        size
      ) do
    enc_bf = encode({:bloom_filter, bloom_filter})
    bf_pos = offset + @separator_size
    bf_size = byte_size(enc_bf)
    enc_key_range = encode({:key_range, key_range})
    key_range_pos = bf_pos + bf_size
    key_range_size = byte_size(enc_key_range)
    enc_priority = encode({:priority, priority})
    priority_pos = key_range_pos + key_range_size
    priority_size = byte_size(enc_priority)

    size =
      size +
        bf_size +
        key_range_size +
        priority_size +
        @metadata_size +
        @separator_size +
        @magic_size

    metadata =
      <<
        level_key::integer-32,
        bf_pos::integer-64,
        bf_size::integer-64,
        key_range_pos::integer-64,
        key_range_size::integer-64,
        priority_pos::integer-64,
        priority_size::integer-64,
        no_of_blocks::integer-64,
        size::integer-64,
        offset::integer-64
      >>

    <<
      @separator::binary,
      enc_bf::binary,
      enc_key_range::binary,
      enc_priority::binary,
      metadata::binary,
      @magic::binary
    >>
  end

  @spec decode_block(binary()) :: {:ok, triple()} | {:error, :invalid_block}
  def decode_block(<<@block_id, _span::integer-16, triple::binary>>) do
    {:ok, decode(triple)}
  end

  def decode_block(_), do: {:error, :invalid_block}

  # @spec decode_metadata(binary()) ::
  #         {:ok, {level(), size(), position(), size(), position(), no_of_blocks(), offset()}}
  #         | {:error, :invalid_metadata}
  def decode_metadata(<<
        level_key::integer-32,
        bf_pos::integer-64,
        bf_size::integer-64,
        key_range_pos::integer-64,
        key_range_size::integer-64,
        priority_pos::integer-64,
        priority_size::integer-64,
        no_of_blocks::integer-64,
        size::integer-64,
        offset::integer-64
      >>) do
    {:ok,
     {
       level_key,
       bf_pos,
       bf_size,
       key_range_pos,
       key_range_size,
       priority_pos,
       priority_size,
       no_of_blocks,
       size,
       offset
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

  # @spec decode_range(binary()) ::
  #         {:ok, range()}
  #         | {:error, :invalid_range}
  def decode_key_range(encoded) do
    case decode(encoded) do
      {:key_range, {_, _} = key_range} -> {:ok, key_range}
      _ -> {:error, :invalid_key_range}
    end
  end

  def decode_priority(encoded) do
    case decode(encoded) do
      {:priority, priority} -> {:ok, priority}
      _ -> {:error, :invalid_priority}
    end
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
