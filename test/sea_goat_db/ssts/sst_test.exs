defmodule SeaGoatDB.SSTs.SSTTest do
  use ExUnit.Case, async: true
  alias SeaGoatDB.SSTs.SST

  test "span/1 returns the multiple of 512 bytes the block spans" do
    1..2048
    |> Enum.chunk_every(512)
    |> Enum.zip([1, 2, 3, 4])
    |> Enum.each(fn {chunk, span} ->
      for n <- chunk, do: assert(span == SST.span(n))
    end)
  end

  # test "encode_block/3 and decode_block/1 works as expected" do
  # end

  # test "encode_block/2 returns a binary with size a multiple of 512 bytes and can be decoded" do
  #   assert <<"SEAGOATDBBLOCK00", 1::integer-16, _data::binary>> =
  #            block = SST.encode_block("k", "v")
  #
  #   assert byte_size(block) == 512
  #   assert {:ok, {"k", "v"}} == SST.decode_block(block)
  #
  #   value = String.duplicate("v", 600)
  #
  #   assert <<"SEAGOATDBBLOCK00", 2::integer-16, _data::binary>> =
  #            block = SST.encode_block("k", value)
  #
  #   assert byte_size(block) == 2 * 512
  #   assert {:ok, {"k", ^value}} = SST.decode_block(block)
  # end
  #
  # test "encode_footer/5 returns a binary with all information that can be decoded" do
  #   level = 0
  #   bf = %SeaGoatDB.BloomFilter{}
  #   range = {1, 2}
  #   min_seq = 3
  #   max_seq = 4
  #   no_of_blocks = 5
  #   offset = 6
  #
  #   bin = SST.encode_footer(level, bf, range, min_seq, max_seq, offset, no_of_blocks)
  #
  #   assert "SEAGOATDBFILE000" == :binary.part(bin, {byte_size(bin), -SST.size(:magic)})
  #
  #   metadata =
  #     :binary.part(bin, {byte_size(bin) - SST.size(:magic), -SST.size(:metadata)})
  #
  #   assert {:ok,
  #           {
  #             ^level,
  #             bf_pos,
  #             bf_size,
  #             range_pos,
  #             range_size,
  #             min_seq_pos,
  #             min_seq_size,
  #             max_seq_pos,
  #             max_seq_size,
  #             ^no_of_blocks,
  #             ^offset
  #           }} = SST.decode_metadata(metadata)
  #
  #   assert {:ok, ^bf} =
  #            :binary.part(bin, bf_pos - offset, bf_size) |> SST.decode_bloom_filter()
  #
  #   assert {:ok, ^range} =
  #            :binary.part(bin, range_pos - offset, range_size) |> SST.decode_range()
  #
  #   assert {:ok, ^min_seq} =
  #            :binary.part(bin, min_seq_pos - offset, min_seq_size) |> SST.decode_sequence()
  #
  #   assert {:ok, ^max_seq} =
  #            :binary.part(bin, max_seq_pos - offset, max_seq_size) |> SST.decode_sequence()
  # end
end
