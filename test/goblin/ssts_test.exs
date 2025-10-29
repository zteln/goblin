defmodule Goblin.SSTsTest do
  use ExUnit.Case, async: true
  alias Goblin.SSTs
  alias Goblin.BloomFilter

  @moduletag :tmp_dir

  describe "flush/3 and find/2" do
    test "writes SST file and can find keys", c do
      file = Path.join(c.tmp_dir, "test.goblin")
      level_key = 0

      data = [
        {1, "key1", "value1"},
        {2, "key2", "value2"},
        {3, "key3", "value3"}
      ]

      assert {:ok,
              [
                %{
                  file: file,
                  bloom_filter: bloom_filter,
                  priority: priority,
                  size: size,
                  key_range: key_range
                }
              ]} =
               SSTs.flush(data, level_key, 10, fn -> file end)

      assert %BloomFilter{} = bloom_filter
      assert priority == 1
      assert size > 0
      assert key_range == {"key1", "key3"}
      assert File.exists?(file)

      assert {:ok, {:value, 1, "value1"}} = SSTs.find(file, "key1")
      assert {:ok, {:value, 2, "value2"}} = SSTs.find(file, "key2")
      assert {:ok, {:value, 3, "value3"}} = SSTs.find(file, "key3")
    end

    test "flushes data to multiple SST files when exceeding key_limit", c do
      level_key = 0
      key_limit = 5

      data = [
        {1, "key1", "value1"},
        {2, "key2", "value2"},
        {3, "key3", "value3"},
        {4, "key4", "value4"},
        {5, "key5", "value5"},
        {6, "key6", "value6"},
        {7, "key7", "value7"},
        {8, "key8", "value8"}
      ]

      counter = :counters.new(1, [])

      file_getter = fn ->
        n = :counters.get(counter, 1)
        :counters.add(counter, 1, 1)
        Path.join(c.tmp_dir, "flush_#{n}.goblin")
      end

      assert {:ok, flushed} = SSTs.flush(data, level_key, key_limit, file_getter)

      assert length(flushed) == 2

      for {file, {_bf, _priority, _size, _range}} <- flushed do
        assert File.exists?(file)
      end
    end

    test "find returns error for non-existent key", c do
      file = Path.join(c.tmp_dir, "test.goblin")
      level_key = 0

      data = [
        {1, "key1", "value1"},
        {2, "key2", "value2"}
      ]

      assert {:ok, [_]} = SSTs.flush(data, level_key, 10, fn -> file end)

      assert :error = SSTs.find(file, "key3")
      assert :error = SSTs.find(file, "key0")
      assert :error = SSTs.find(file, "key99")
    end

    test "writes SST with large values spanning multiple blocks", c do
      file = Path.join(c.tmp_dir, "large.goblin")
      level_key = 0

      large_value = String.duplicate("x", 1000)

      data = [
        {1, "key1", large_value}
      ]

      assert {:ok, [_sst]} =
               SSTs.flush(data, level_key, 10, fn -> file end)

      assert {:ok, {:value, 1, ^large_value}} = SSTs.find(file, "key1")
    end

    test "writes SST with tombstone values", c do
      file = Path.join(c.tmp_dir, "tombstone.goblin")
      level_key = 0

      data = [
        {1, "key1", "value1"},
        {2, "key2", :tombstone},
        {3, "key3", "value3"}
      ]

      assert {:ok, [_sst]} = SSTs.flush(data, level_key, 10, fn -> file end)

      assert {:ok, {:value, 1, "value1"}} = SSTs.find(file, "key1")
      assert {:ok, {:value, 2, nil}} = SSTs.find(file, "key2")
      assert {:ok, {:value, 3, "value3"}} = SSTs.find(file, "key3")
    end

    test "writes SST with many keys", c do
      file = Path.join(c.tmp_dir, "many.goblin")
      level_key = 0

      data =
        for n <- 1..100 do
          key = String.pad_leading("#{n}", 3, "0")
          {n, key, "value#{n}"}
        end

      assert {:ok, [%{priority: priority, key_range: key_range}]} =
               SSTs.flush(data, level_key, 100, fn -> file end)

      assert priority == 1
      assert key_range == {"001", "100"}

      for n <- 1..100 do
        key = String.pad_leading("#{n}", 3, "0")
        value = "value#{n}"
        assert {:ok, {:value, ^n, ^value}} = SSTs.find(file, key)
      end
    end

    test "writes SST with different level keys", c do
      for level_key <- 0..3 do
        file = Path.join(c.tmp_dir, "level#{level_key}.goblin")

        data = [
          {1, "key1", "value1"}
        ]

        assert {:ok, [_sst]} = SSTs.flush(data, level_key, 100, fn -> file end)
        assert {:ok, %{level_key: ^level_key}} = SSTs.fetch_sst(file)
      end
    end
  end

  describe "fetch_sst/1" do
    test "returns bloom filter and metadata", c do
      file = Path.join(c.tmp_dir, "info.goblin")
      level_key = 2

      data = [
        {5, "key1", "value1"},
        {6, "key2", "value2"}
      ]

      SSTs.flush(data, level_key, 100, fn -> file end)

      assert {:ok,
              %{
                level_key: ^level_key,
                priority: priority,
                size: size,
                key_range: key_range,
                bloom_filter: bloom_filter
              }} =
               SSTs.fetch_sst(file)

      assert %BloomFilter{} = bloom_filter
      assert priority == 5
      assert size > 0
      assert key_range == {"key1", "key2"}
    end

    test "returns error for non-SST file", c do
      file = Path.join(c.tmp_dir, "not_sst.txt")
      File.write!(file, "not an SST")

      assert {:error, :not_an_ss_table} = SSTs.fetch_sst(file)
    end

    test "returns error for non-existent file", c do
      file = Path.join(c.tmp_dir, "nonexistent.goblin")

      assert_raise RuntimeError, fn ->
        SSTs.fetch_sst(file)
      end
    end
  end

  describe "stream!/1" do
    test "streams all keys in order", c do
      file = Path.join(c.tmp_dir, "stream.goblin")
      level_key = 0

      data = [
        {1, "a", "value_a"},
        {2, "b", "value_b"},
        {3, "c", "value_c"}
      ]

      SSTs.flush(data, level_key, 100, fn -> file end)

      result = SSTs.stream!(file) |> Enum.to_list()

      assert result == data
    end

    test "streams large number of keys", c do
      file = Path.join(c.tmp_dir, "stream_large.goblin")
      level_key = 0

      data =
        for n <- 1..50 do
          {n, "key#{String.pad_leading("#{n}", 3, "0")}", "value#{n}"}
        end

      SSTs.flush(data, level_key, 100, fn -> file end)

      result = SSTs.stream!(file) |> Enum.to_list()

      assert length(result) == 50
      assert result == data
    end
  end

  describe "iterate/1" do
    test "iterates through SST file", c do
      file = Path.join(c.tmp_dir, "iterate.goblin")
      level_key = 0

      data = [
        {1, "key1", "value1"},
        {2, "key2", "value2"},
        {3, "key3", "value3"}
      ]

      SSTs.flush(data, level_key, 100, fn -> file end)

      iter = SSTs.iterate(file)
      assert {{1, "key1", "value1"}, iter} = SSTs.iterate(iter)
      assert {{2, "key2", "value2"}, iter} = SSTs.iterate(iter)
      assert {{3, "key3", "value3"}, iter} = SSTs.iterate(iter)
      assert :ok = SSTs.iterate(iter)
    end
  end

  describe "delete/1" do
    test "deletes SST file", c do
      file = Path.join(c.tmp_dir, "delete.goblin")
      level_key = 0

      data = [{1, "key1", "value1"}]
      SSTs.flush(data, level_key, 100, fn -> file end)

      assert File.exists?(file)
      assert :ok = SSTs.delete(file)
      refute File.exists?(file)
    end

    test "delete non-existent file returns ok", c do
      file = Path.join(c.tmp_dir, "nonexistent.goblin")

      refute File.exists?(file)
      assert :ok = SSTs.delete(file)
    end
  end

  describe "complex data types" do
    test "writes and reads complex Elixir terms", c do
      file = Path.join(c.tmp_dir, "complex.goblin")
      level_key = 0

      data = [
        {1, {:compound, "key"}, %{nested: [1, 2, 3]}},
        {2, %{map: "key"}, [nested: %{data: "value"}]}
      ]

      SSTs.flush(data, level_key, 100, fn -> file end)

      assert {:ok, {:value, 1, %{nested: [1, 2, 3]}}} = SSTs.find(file, {:compound, "key"})
      assert {:ok, {:value, 2, [nested: %{data: "value"}]}} = SSTs.find(file, %{map: "key"})
    end
  end
end
