defmodule Goblin.DiskTableTest do
  use ExUnit.Case, async: true
  import TestHelper
  alias Goblin.DiskTable
  alias Goblin.BloomFilter

  @moduletag :tmp_dir

  describe "new/3 and find/2" do
    test "writes SST file and can find keys", c do
      file = Path.join(c.tmp_dir, "test.goblin")
      level_key = 0

      stream =
        [
          {"key1", 1, "value1"},
          {"key2", 2, "value2"},
          {"key3", 3, "value3"}
        ]
        |> stream_flush_data(10)

      assert {:ok,
              [
                %{
                  file: file,
                  bloom_filter: bloom_filter,
                  seq_range: seq_range,
                  size: size,
                  key_range: key_range
                }
              ]} =
               DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert %BloomFilter{} = bloom_filter
      assert seq_range == {1, 3}
      assert size > 0
      assert key_range == {"key1", "key3"}
      assert File.exists?(file)

      assert {:ok, {:value, 1, "value1"}} = DiskTable.find(file, "key1")
      assert {:ok, {:value, 2, "value2"}} = DiskTable.find(file, "key2")
      assert {:ok, {:value, 3, "value3"}} = DiskTable.find(file, "key3")
    end

    test "flushes data to multiple SST files when exceeding key_limit", c do
      level_key = 0
      key_limit = 5

      stream =
        [
          {"key1", 1, "value1"},
          {"key2", 2, "value2"},
          {"key3", 3, "value3"},
          {"key4", 4, "value4"},
          {"key5", 5, "value5"},
          {"key6", 6, "value6"},
          {"key7", 7, "value7"},
          {"key8", 8, "value8"}
        ]
        |> stream_flush_data(key_limit)

      counter = :counters.new(1, [])

      file_getter = fn ->
        n = :counters.get(counter, 1)
        :counters.add(counter, 1, 1)
        Path.join(c.tmp_dir, "flush_#{n}.goblin")
      end

      assert {:ok, flushed} = DiskTable.new([stream], level_key, file_getter: file_getter)

      assert length(flushed) == 2

      for {file, {_bf, _priority, _size, _range}} <- flushed do
        assert File.exists?(file)
      end
    end

    test "find/2 returns :not_found for non-existent key", c do
      file = Path.join(c.tmp_dir, "test.goblin")
      level_key = 0

      stream =
        [
          {"key1", 1, "value1"},
          {"key2", 2, "value2"},
          {"key4", 3, "value4"}
        ]
        |> stream_flush_data(10)

      assert {:ok, [_]} = DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert :not_found = DiskTable.find(file, "key3")
      assert :not_found = DiskTable.find(file, "key0")
      assert :not_found = DiskTable.find(file, "key99")
    end

    test "writes SST with large values spanning multiple blocks", c do
      file = Path.join(c.tmp_dir, "large.goblin")
      level_key = 0

      large_value = String.duplicate("x", 1000)

      stream =
        [
          {"key1", 1, large_value}
        ]
        |> stream_flush_data(10)

      assert {:ok, [_sst]} =
               DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert {:ok, {:value, 1, ^large_value}} = DiskTable.find(file, "key1")
    end

    test "writes SST with tombstone values", c do
      file = Path.join(c.tmp_dir, "tombstone.goblin")
      level_key = 0

      stream =
        [
          {"key1", 1, "value1"},
          {"key2", 2, :"$goblin_tombstone"},
          {"key3", 3, "value3"}
        ]
        |> stream_flush_data(10)

      assert {:ok, [_sst]} = DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert {:ok, {:value, 1, "value1"}} = DiskTable.find(file, "key1")
      assert {:ok, {:value, 2, :"$goblin_tombstone"}} = DiskTable.find(file, "key2")
      assert {:ok, {:value, 3, "value3"}} = DiskTable.find(file, "key3")
    end

    test "writes SST with many keys", c do
      file = Path.join(c.tmp_dir, "many.goblin")
      level_key = 0

      stream =
        for n <- 1..100 do
          key = String.pad_leading("#{n}", 3, "0")
          {key, n, "value#{n}"}
        end
        |> stream_flush_data(100)

      assert {:ok, [%{seq_range: seq_range, key_range: key_range}]} =
               DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert seq_range == {1, 100}
      assert key_range == {"001", "100"}

      for n <- 1..100 do
        key = String.pad_leading("#{n}", 3, "0")
        value = "value#{n}"
        assert {:ok, {:value, ^n, ^value}} = DiskTable.find(file, key)
      end
    end

    test "writes SST with different level keys", c do
      for level_key <- 0..3 do
        file = Path.join(c.tmp_dir, "level#{level_key}.goblin")

        stream =
          [
            {"key1", 1, "value1"}
          ]
          |> stream_flush_data(100)

        assert {:ok, [_sst]} = DiskTable.new([stream], level_key, file_getter: fn -> file end)
        assert {:ok, %{level_key: ^level_key}} = DiskTable.fetch_sst(file)
      end
    end
  end

  describe "fetch_sst/1" do
    test "returns bloom filter and metadata", c do
      file = Path.join(c.tmp_dir, "info.goblin")
      level_key = 2

      stream =
        [
          {"key1", 5, "value1"},
          {"key2", 6, "value2"}
        ]
        |> stream_flush_data(100)

      DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert {:ok,
              %{
                level_key: ^level_key,
                seq_range: seq_range,
                size: size,
                key_range: key_range,
                bloom_filter: bloom_filter
              }} =
               DiskTable.fetch_sst(file)

      assert %BloomFilter{} = bloom_filter
      assert seq_range == {5, 6}
      assert size > 0
      assert key_range == {"key1", "key2"}
    end

    test "returns error for non-SST file", c do
      file = Path.join(c.tmp_dir, "not_sst.txt")
      File.write!(file, "not an SST")

      assert {:error, :not_an_ss_table} = DiskTable.fetch_sst(file)
    end

    test "returns error for non-existent file", c do
      file = Path.join(c.tmp_dir, "nonexistent.goblin")

      assert_raise RuntimeError, fn ->
        DiskTable.fetch_sst(file)
      end
    end
  end

  describe "stream!/1" do
    test "streams all keys in order", c do
      file = Path.join(c.tmp_dir, "stream.goblin")
      level_key = 0

      data = [
        {"a", 1, "value_a"},
        {"b", 2, "value_b"},
        {"c", 3, "value_c"}
      ]

      stream = stream_flush_data(data, 100)

      DiskTable.new([stream], level_key, file_getter: fn -> file end)

      result = DiskTable.stream!(file) |> Enum.to_list()

      assert result == data
    end

    test "streams large number of keys", c do
      file = Path.join(c.tmp_dir, "stream_large.goblin")
      level_key = 0

      data =
        for n <- 1..50 do
          {n, "key#{String.pad_leading("#{n}", 3, "0")}", "value#{n}"}
        end

      stream = stream_flush_data(data, 100)

      DiskTable.new([stream], level_key, file_getter: fn -> file end)

      result = DiskTable.stream!(file) |> Enum.to_list()

      assert length(result) == 50
      assert result == data
    end
  end

  describe "iterate/1" do
    test "iterates through SST file", c do
      file = Path.join(c.tmp_dir, "iterate.goblin")
      level_key = 0

      stream =
        [
          {"key1", 1, "value1"},
          {"key2", 2, "value2"},
          {"key3", 3, "value3"}
        ]
        |> stream_flush_data(100)

      DiskTable.new([stream], level_key, file_getter: fn -> file end)

      {disk, iterator, closer} = DiskTable.iterator(file)
      assert {{"key1", 1, "value1"}, disk} = iterator.(disk)
      assert {{"key2", 2, "value2"}, disk} = iterator.(disk)
      assert {{"key3", 3, "value3"}, disk} = iterator.(disk)
      assert :ok == iterator.(disk)
      assert :ok == closer.(disk)
    end
  end

  describe "delete/1" do
    test "deletes SST file", c do
      file = Path.join(c.tmp_dir, "delete.goblin")
      level_key = 0

      stream = [{"key1", 1, "value1"}] |> stream_flush_data(100)
      DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert File.exists?(file)
      assert :ok = DiskTable.delete(file)
      refute File.exists?(file)
    end

    test "delete non-existent file returns ok", c do
      file = Path.join(c.tmp_dir, "nonexistent.goblin")

      refute File.exists?(file)
      assert :ok = DiskTable.delete(file)
    end
  end

  describe "complex data types" do
    test "writes and reads complex Elixir terms", c do
      file = Path.join(c.tmp_dir, "complex.goblin")
      level_key = 0

      stream =
        [
          {{:compound, "key"}, 1, %{nested: [1, 2, 3]}},
          {%{map: "key"}, 2, [nested: %{data: "value"}]}
        ]
        |> stream_flush_data(100)

      DiskTable.new([stream], level_key, file_getter: fn -> file end)

      assert {:ok, {:value, 1, %{nested: [1, 2, 3]}}} = DiskTable.find(file, {:compound, "key"})
      assert {:ok, {:value, 2, [nested: %{data: "value"}]}} = DiskTable.find(file, %{map: "key"})
    end
  end
end
