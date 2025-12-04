defmodule Goblin.DiskTable.SSTTest do
  use ExUnit.Case, async: true
  alias Goblin.DiskTable.SST

  describe "valid_magic?/1" do
    test "returns true for valid magic bytes" do
      assert SST.valid_magic?("GOBLINFILE000000")
    end

    test "returns false for invalid magic bytes" do
      refute SST.valid_magic?("INVALID")
      refute SST.valid_magic?("")
      refute SST.valid_magic?("GOBLINFILE000001")
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

  describe "add_data/3, decode_block/1" do
    test "updates SST and returns valid SST block" do
      sst = SST.new("foo", 0)

      assert {block, sst} = SST.add_data(sst, {:k, 0, :v}, false)

      assert %{key_range: {:k, :k}} = sst
      assert %{seq_range: {0, 0}} = sst
      assert {:ok, {:k, 0, :v}} == SST.decode_block(block)
    end

    test "compresses blocks" do
      sst = SST.new("foo", 0)

      data = {:k, 0, List.duplicate(:v, 1024)}

      assert {compressed_block, _sst} = SST.add_data(sst, data, true)
      assert {block, _sst} = SST.add_data(sst, data, false)

      assert byte_size(compressed_block) < byte_size(block)
    end
  end

  describe "add_metadata/4, decode_metadata/1" do
    test "updates SST and returns metadata block" do
      level_key = 0
      offset = 100
      sst = SST.new("foo", level_key)
      {_block, sst} = SST.add_data(sst, {:k1, 0, :v1}, false)
      {_block, sst} = SST.add_data(sst, {:k2, 1, :v2}, false)

      assert {metadata_block, sst} = SST.add_metadata(sst, offset, 0.01, false)
      assert %{size: size, bloom_filter: bf} = sst

      assert Goblin.BloomFilter.is_member(bf, :k1)
      assert Goblin.BloomFilter.is_member(bf, :k2)

      metadata_offset = byte_size(metadata_block) - SST.size(:magic) - SST.size(:metadata)
      metadata = :binary.part(metadata_block, metadata_offset, SST.size(:metadata))

      assert {:ok,
              {
                ^level_key,
                bf_pos,
                bf_size,
                key_range_pos,
                key_range_size,
                seq_range_pos,
                seq_range_size,
                2,
                ^size,
                ^offset,
                _
              }} = SST.decode_metadata(metadata)

      assert {:ok, %Goblin.BloomFilter{} = bf} =
               :binary.part(metadata_block, bf_pos - offset, bf_size) |> SST.decode_bloom_filter()

      assert Goblin.BloomFilter.is_member(bf, :k1)
      assert Goblin.BloomFilter.is_member(bf, :k2)

      assert {:ok, {:k1, :k2}} ==
               :binary.part(metadata_block, key_range_pos - offset, key_range_size)
               |> SST.decode_key_range()

      assert {:ok, {0, 1}} ==
               :binary.part(metadata_block, seq_range_pos - offset, seq_range_size)
               |> SST.decode_seq_range()
    end
  end
end
