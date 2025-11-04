defmodule Goblin.SSTs.SSTTest do
  use ExUnit.Case, async: true
  alias Goblin.SSTs.SST
  alias Goblin.BloomFilter

  describe "is_ss_table/1" do
    test "returns true for valid magic bytes" do
      assert SST.is_ss_table("GOBLINFILE000000")
    end

    test "returns false for invalid magic bytes" do
      refute SST.is_ss_table("INVALID")
      refute SST.is_ss_table("")
      refute SST.is_ss_table("GOBLINFILE000001")
    end
  end

  describe "block_span/1" do
    test "returns span for valid block header" do
      assert {:ok, 1} = SST.block_span(<<"GOBLINBLOCK00000", 1::integer-16>>)
      assert {:ok, 5} = SST.block_span(<<"GOBLINBLOCK00000", 5::integer-16>>)
      assert {:ok, 100} = SST.block_span(<<"GOBLINBLOCK00000", 100::integer-16>>)
    end

    test "returns error for separator" do
      assert {:error, :eod} = SST.block_span(<<"GOBLINSEP0000000", "rest">>)
    end

    test "returns error for invalid block header" do
      assert {:error, :not_block_start} = SST.block_span("INVALID")
      assert {:error, :not_block_start} = SST.block_span("")
      assert {:error, :not_block_start} = SST.block_span(<<"WRONGBLOCK00", 1::integer-16>>)
    end
  end

  describe "span/1" do
    test "returns the multiple of 512 bytes the block spans" do
      1..2048
      |> Enum.chunk_every(512)
      |> Enum.zip([1, 2, 3, 4])
      |> Enum.each(fn {chunk, span} ->
        for n <- chunk, do: assert(span == SST.span(n))
      end)
    end

    test "returns 1 for sizes 1-512" do
      assert SST.span(1) == 1
      assert SST.span(256) == 1
      assert SST.span(512) == 1
    end

    test "returns 2 for sizes 513-1024" do
      assert SST.span(513) == 2
      assert SST.span(768) == 2
      assert SST.span(1024) == 2
    end
  end

  describe "size/1" do
    test "returns correct size for block" do
      assert SST.size(:block) == 512
    end

    test "returns correct size for magic" do
      assert SST.size(:magic) == 16
    end

    test "returns correct size for separator" do
      assert SST.size(:separator) == 16
    end

    test "returns correct size for metadata" do
      assert SST.size(:metadata) == 76
    end

    test "returns correct size for block_header" do
      assert SST.size(:block_header) == 18
    end
  end

  describe "encode_block/3 and decode_block/1" do
    test "encodes and decodes a simple block" do
      seq = 1
      key = "test_key"
      value = "test_value"

      block = SST.encode_block(seq, key, value)

      assert <<"GOBLINBLOCK00000", span::integer-16, _rest::binary>> = block
      assert span == 1
      assert byte_size(block) == 512

      assert {:ok, {^seq, ^key, ^value}} = SST.decode_block(block)
    end

    test "encodes and decodes a block with large value" do
      seq = 42
      key = "large_key"
      value = String.duplicate("v", 600)

      block = SST.encode_block(seq, key, value)

      assert <<"GOBLINBLOCK00000", span::integer-16, _rest::binary>> = block
      assert span == 2
      assert byte_size(block) == 1024

      assert {:ok, {^seq, ^key, ^value}} = SST.decode_block(block)
    end

    test "encodes and decodes a block with complex terms" do
      seq = 100
      key = {:compound, "key", 123}
      value = %{data: [1, 2, 3], nested: %{foo: "bar"}}

      block = SST.encode_block(seq, key, value)

      assert {:ok, {^seq, ^key, ^value}} = SST.decode_block(block)
    end

    test "decode_block returns error for invalid block" do
      assert {:error, :invalid_block} = SST.decode_block("INVALID")
      assert {:error, :invalid_block} = SST.decode_block("")
      assert {:error, :invalid_block} = SST.decode_block(<<"WRONGBLOCK00", 1::integer-16>>)
    end
  end

  describe "encode_footer/7 and decode_metadata/1" do
    test "encodes and decodes footer metadata" do
      level_key = 2
      bloom_filter = BloomFilter.new() |> BloomFilter.put("key1") |> BloomFilter.generate()
      key_range = {"a", "z"}
      priority = 1000
      offset = 5120
      no_of_blocks = 10
      size = 5120

      footer =
        SST.encode_footer(
          level_key,
          bloom_filter,
          key_range,
          priority,
          offset,
          no_of_blocks,
          size
        )

      assert <<"GOBLINSEP0000000", _rest::binary>> = footer
      assert :binary.part(footer, {byte_size(footer), -16}) == "GOBLINFILE000000"

      metadata_offset = byte_size(footer) - SST.size(:magic) - SST.size(:metadata)
      metadata = :binary.part(footer, metadata_offset, SST.size(:metadata))

      assert {:ok,
              {
                ^level_key,
                bf_pos,
                bf_size,
                key_range_pos,
                key_range_size,
                priority_pos,
                priority_size,
                ^no_of_blocks,
                total_size,
                ^offset
              }} = SST.decode_metadata(metadata)

      assert bf_pos > offset
      assert bf_size > 0
      assert key_range_pos > bf_pos
      assert key_range_size > 0
      assert priority_pos > key_range_pos
      assert priority_size > 0
      assert total_size > size
    end

    test "decode_metadata returns error for invalid metadata" do
      assert {:error, :invalid_metadata} = SST.decode_metadata("INVALID")
      assert {:error, :invalid_metadata} = SST.decode_metadata("")
      assert {:error, :invalid_metadata} = SST.decode_metadata(<<1, 2, 3>>)
    end
  end

  describe "decode_bloom_filter/1" do
    test "decodes valid bloom filter" do
      bloom_filter = BloomFilter.new() |> BloomFilter.put("key1") |> BloomFilter.generate()
      encoded = :erlang.term_to_binary({:bloom_filter, BloomFilter.to_tuple(bloom_filter)})

      assert {:ok, decoded_bf} = SST.decode_bloom_filter(encoded)
      assert %BloomFilter{} = decoded_bf
    end

    test "returns error for invalid bloom filter" do
      invalid_encoded = :erlang.term_to_binary({:not_bloom_filter, "data"})
      assert {:error, :invalid_bloom_filter} = SST.decode_bloom_filter(invalid_encoded)
    end
  end

  describe "decode_key_range/1" do
    test "decodes valid key range" do
      key_range = {"min_key", "max_key"}
      encoded = :erlang.term_to_binary({:key_range, key_range})

      assert {:ok, ^key_range} = SST.decode_key_range(encoded)
    end

    test "returns error for invalid key range" do
      invalid_encoded = :erlang.term_to_binary({:not_key_range, "data"})
      assert {:error, :invalid_key_range} = SST.decode_key_range(invalid_encoded)
    end
  end

  describe "decode_priority/1" do
    test "decodes valid priority" do
      priority = 12345
      encoded = :erlang.term_to_binary({:priority, priority})

      assert {:ok, ^priority} = SST.decode_priority(encoded)
    end

    test "returns error for invalid priority" do
      invalid_encoded = :erlang.term_to_binary({:not_priority, "data"})
      assert {:error, :invalid_priority} = SST.decode_priority(invalid_encoded)
    end
  end

  describe "full footer encode/decode integration" do
    test "encodes footer and decodes all components" do
      level_key = 1
      bloom_filter = BloomFilter.new() |> BloomFilter.put("test") |> BloomFilter.generate()
      key_range = {"aaa", "zzz"}
      priority = 5000
      offset = 2048
      no_of_blocks = 4
      size = 2048

      footer =
        SST.encode_footer(
          level_key,
          bloom_filter,
          key_range,
          priority,
          offset,
          no_of_blocks,
          size
        )

      metadata_offset = byte_size(footer) - SST.size(:magic) - SST.size(:metadata)
      metadata = :binary.part(footer, metadata_offset, SST.size(:metadata))

      assert {:ok,
              {
                ^level_key,
                bf_pos,
                bf_size,
                key_range_pos,
                key_range_size,
                priority_pos,
                priority_size,
                ^no_of_blocks,
                _total_size,
                ^offset
              }} = SST.decode_metadata(metadata)

      bf_binary = :binary.part(footer, bf_pos - offset, bf_size)
      assert {:ok, decoded_bf} = SST.decode_bloom_filter(bf_binary)
      assert %BloomFilter{} = decoded_bf

      key_range_binary = :binary.part(footer, key_range_pos - offset, key_range_size)
      assert {:ok, ^key_range} = SST.decode_key_range(key_range_binary)

      priority_binary = :binary.part(footer, priority_pos - offset, priority_size)
      assert {:ok, ^priority} = SST.decode_priority(priority_binary)
    end
  end
end
