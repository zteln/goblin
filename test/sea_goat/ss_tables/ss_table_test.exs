defmodule SeaGoat.SSTables.SSTableTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables.SSTable

  test "span/1 returns the multiple of 512 bytes the block spans" do
    1..2048
    |> Enum.chunk_every(512)
    |> Enum.zip([1, 2, 3, 4])
    |> Enum.each(fn {chunk, span} ->
      for n <- chunk, do: assert(span == SSTable.span(n))
    end)
  end

  test "encode_block/2 returns a binary with size a multiple of 512 bytes" do
    key = "k"
    value = "v"
    data = :erlang.term_to_binary({key, value})

    assert <<"SEAGOATDBBLOCK00", 1::integer-16, ^data::binary, 0::size(479)-unit(8)>> =
             SSTable.encode_block(key, value)

    value = String.duplicate("v", 500)
    data = :erlang.term_to_binary({key, value})

    assert <<"SEAGOATDBBLOCK00", 2::integer-16, ^data::binary, 0::size(492)-unit(8)>> =
             SSTable.encode_block(key, value)
  end

  test "encode_footer/5 returns a binary with all information" do
    level = 1
    bf = %SeaGoat.BloomFilter{}
    enc_bf = {:bloom_filter, bf} |> :erlang.term_to_binary()
    enc_bf_size = byte_size(enc_bf)
    range = {1, 2}
    enc_range = {:range, range} |> :erlang.term_to_binary()
    enc_range_size = byte_size(enc_range)
    no_of_blocks = 2
    offset = 3

    assert <<
             _separator::binary-size(16),
             ^enc_bf::binary,
             ^enc_range::binary,
             level::integer-32,
             bf_size::integer-64,
             bf_pos::integer-64,
             range_size::integer-64,
             range_pos::integer-64,
             enc_no_of_blocks::integer-64,
             enc_offset::integer-64,
             _magic::binary-size(16)
           >> =
             SSTable.encode_footer(level, bf, range, offset, no_of_blocks)

    assert level == 1
    assert bf_size == enc_bf_size
    assert bf_pos == 3 + 16
    assert range_size == enc_range_size
    assert range_pos == bf_pos + bf_size
    assert enc_no_of_blocks == no_of_blocks
    assert enc_offset == offset
  end

  test "decode_block/1 returns {:ok, block} with valid binary, {:error, :invalid_block} on invalid binary" do
    key = "k"
    value = "v"
    data = :erlang.term_to_binary({key, value})
    block = <<"SEAGOATDBBLOCK00", 1::integer-16, data::binary, 0::size(479)-unit(8)>>

    assert {:ok, {"k", "v"}} == SSTable.decode_block(block)
    assert {:error, :invalid_block} == SSTable.decode_block("invalid block")
  end

  test "decode_metadata/1 returns {:ok, metadata} on valid metadata, {:error, :invalid_metadata} on invalid metadata" do
    metadata = <<
      1::integer-32,
      2::integer-64,
      3::integer-64,
      4::integer-64,
      5::integer-64,
      6::integer-64,
      7::integer-64
    >>

    assert {:ok, {1, 2, 3, 4, 5, 6, 7}} == SSTable.decode_metadata(metadata)
    assert {:error, :invalid_metadata} == SSTable.decode_metadata("invalid metadata")
  end

  test "decode_bloom_filter/1 returns {:ok, bloom_filter} on valid Bloom filter, {:error, :invalid_bloom_filter} on invalid Bloom filter" do
    bf = SeaGoat.BloomFilter.new()
    encoded = :erlang.term_to_binary({:bloom_filter, bf})
    assert {:ok, ^bf} = SSTable.decode_bloom_filter(encoded)

    encoded = :erlang.term_to_binary({:bloom_filter, :term})
    assert {:error, :invalid_bloom_filter} == SSTable.decode_bloom_filter(encoded)
  end

  test "decode_range/1 returns {:ok, range} if valid, otherwise {:error, :invalid_range}" do
    range = {1, 2}
    encoded = :erlang.term_to_binary({:range, range})
    assert {:ok, ^range} = SSTable.decode_range(encoded)

    encoded = :erlang.term_to_binary({:range, :term})
    assert {:error, :invalid_range} == SSTable.decode_range(encoded)
  end
end
