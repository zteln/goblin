defmodule Goblin.DiskTables.EncoderTest do
  use ExUnit.Case, async: true

  describe "encode_sst_block/2, decode_sst_header_block/1, decode_sst_block/1" do
    test "can encode and decode triple into SST block" do
      triple = {:key, 0, :val}
      assert {sst_block, 1} = Goblin.DiskTables.Encoder.encode_sst_block(triple, false)

      assert {:ok, 1} ==
               Goblin.DiskTables.Encoder.decode_sst_header_block(:binary.part(sst_block, 0, 18))

      assert {:ok, ^triple} = Goblin.DiskTables.Encoder.decode_sst_block(sst_block)
    end

    test "can encode and decode large triple" do
      triple = {:key, 0, :binary.copy("X", 600)}
      assert {sst_block, 2} = Goblin.DiskTables.Encoder.encode_sst_block(triple, false)

      assert {:ok, 2} ==
               Goblin.DiskTables.Encoder.decode_sst_header_block(
                 :binary.part(sst_block, 0, Goblin.DiskTables.Encoder.sst_header_size())
               )

      assert {:ok, ^triple} = Goblin.DiskTables.Encoder.decode_sst_block(sst_block)
    end

    test "can compress data in SST block" do
      triple = {:key, 0, :binary.copy("X", 600)}

      assert {uncompressed_sst_block, 2} =
               Goblin.DiskTables.Encoder.encode_sst_block(triple, false)

      assert {compressed_sst_block, 1} =
               Goblin.DiskTables.Encoder.encode_sst_block(triple, true)

      assert byte_size(compressed_sst_block) < byte_size(uncompressed_sst_block)
    end

    test "can identify end of SST" do
      {bin, _, _} =
        Goblin.DiskTables.Encoder.encode_footer_block(
          0,
          :bf,
          :key_range,
          :seq_range,
          0,
          1,
          :erlang.crc32(<<>>),
          1,
          false
        )

      assert {:error, :end_of_sst} == Goblin.DiskTables.Encoder.decode_sst_header_block(bin)
    end

    test "can identify invalid SST blocks" do
      assert {:error, :invalid_sst_header} ==
               Goblin.DiskTables.Encoder.decode_sst_header_block("RANDOMBINARY")
    end
  end

  describe "encode_footer_block/9, decode_metadata_block/1, decode_bloom_filter_block/1, decode_key_range_block/1, decode_seq_range_block/1, decode_crc_block/1, decode_size_block/1" do
    test "encodes and decodes correctly" do
      initial_crc = :erlang.crc32(<<>>)

      assert {bin, size, crc} =
               Goblin.DiskTables.Encoder.encode_footer_block(
                 0,
                 :a_bf,
                 :a_key_range,
                 :a_seq_range,
                 0,
                 1,
                 initial_crc,
                 0,
                 false
               )

      blocks =
        :binary.part(
          bin,
          0,
          byte_size(bin) - Goblin.DiskTables.Encoder.magic_size() -
            Goblin.DiskTables.Encoder.size_block_size() -
            Goblin.DiskTables.Encoder.crc_block_size()
        )

      assert :ok ==
               Goblin.DiskTables.Encoder.validate_magic_block(
                 :binary.part(bin, byte_size(bin), -Goblin.DiskTables.Encoder.magic_size())
               )

      assert byte_size(blocks) == size
      assert crc == :erlang.crc32(initial_crc, blocks)

      metadata_block =
        :binary.part(blocks, byte_size(blocks), -Goblin.DiskTables.Encoder.metadata_block_size())

      assert {:ok,
              {0, {bf_block_pos, bf_block_size}, {key_range_block_pos, key_range_block_size},
               {seq_range_block_pos, seq_range_block_size}, 1}} =
               Goblin.DiskTables.Encoder.decode_metadata_block(metadata_block)

      bf_block = :binary.part(blocks, bf_block_pos, bf_block_size)
      key_range_block = :binary.part(blocks, key_range_block_pos, key_range_block_size)
      seq_range_block = :binary.part(blocks, seq_range_block_pos, seq_range_block_size)

      assert {:ok, :a_bf} == Goblin.DiskTables.Encoder.decode_bloom_filter_block(bf_block)

      assert {:ok, :a_key_range} ==
               Goblin.DiskTables.Encoder.decode_key_range_block(key_range_block)

      assert {:ok, :a_seq_range} ==
               Goblin.DiskTables.Encoder.decode_seq_range_block(seq_range_block)
    end

    test "can handle invalid blocks" do
      assert {:error, :invalid_magic} ==
               Goblin.DiskTables.Encoder.validate_magic_block("INVALID_MAGIC")

      assert {:error, :invalid_metadata_block} ==
               Goblin.DiskTables.Encoder.decode_metadata_block("INVALID_METADATA_BLOCK")

      assert {:error, :invalid_bloom_filter_block} ==
               Goblin.DiskTables.Encoder.decode_bloom_filter_block(
                 :erlang.term_to_binary(:invalid_bloom_filter)
               )

      assert {:error, :invalid_key_range_block} ==
               Goblin.DiskTables.Encoder.decode_key_range_block(
                 :erlang.term_to_binary(:invalid_key_range)
               )

      assert {:error, :invalid_seq_range_block} ==
               Goblin.DiskTables.Encoder.decode_seq_range_block(
                 :erlang.term_to_binary(:invalid_seq_range)
               )

      assert {:error, :invalid_crc_block} ==
               Goblin.DiskTables.Encoder.decode_crc_block("INVALID_CRC_BLOCK")

      assert {:error, :invalid_size_block} ==
               Goblin.DiskTables.Encoder.decode_size_block("INVALID_SIZE_BLOCK")
    end
  end
end
