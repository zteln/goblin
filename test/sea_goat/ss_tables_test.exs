defmodule SeaGoat.SSTablesTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables
  alias SeaGoat.SSTables.MemTableIterator
  alias SeaGoat.SSTables.SSTablesIterator
  alias SeaGoat.BloomFilter

  @moduletag :tmp_dir

  test "delete/1 deletes files", c do
    foo = Path.join(c.tmp_dir, "foo")
    bar = Path.join(c.tmp_dir, "bar")
    File.touch(foo)
    assert File.exists?(foo)
    assert :ok == SSTables.delete([foo])
    assert :ok == SSTables.delete([bar])
    refute File.exists?(foo)
    File.touch(foo)
    assert File.exists?(foo)
    assert :ok == SSTables.delete([foo, bar])
    assert :ok == SSTables.delete([])
    refute File.exists?(foo)
  end

  test "switch/2 moves files", c do
    foo = Path.join(c.tmp_dir, "foo")
    bar = Path.join(c.tmp_dir, "bar")
    File.write(foo, "foo")
    File.write(bar, "bar")
    assert File.exists?(foo)
    assert File.exists?(bar)
    assert :ok == SSTables.switch(foo, bar)
    refute File.exists?(foo)
    assert File.exists?(bar)
    assert "foo" == File.read!(bar)
  end

  test "write/4 with MemTableIterator writes an SSTable file on disk", c do
    file = Path.join(c.tmp_dir, "foo")

    data =
      for n <- 1..10, reduce: %{} do
        acc ->
          Map.put(acc, n, "v-#{n}")
      end

    level = 0

    refute File.exists?(file)

    assert {:ok, %BloomFilter{}, ^file, ^level} =
             SSTables.write(%MemTableIterator{}, data, file, level)

    for n <- 1..10 do
      assert {:ok, {:value, "v-#{n}"}} == SSTables.read(file, n)
    end

    assert File.exists?(file)
  end

  test "write/4 with SSTablesIterator writes an SSTable file on disk", c do
    file = Path.join(c.tmp_dir, "foo")
    level = 1

    data =
      for n <- 1..5 do
        file = Path.join(c.tmp_dir, "ss_table_#{n}")

        data =
          for i <- 1..10, reduce: %{} do
            acc ->
              Map.put(acc, n * i, "v-#{n * i}")
          end

        SSTables.write(%MemTableIterator{}, data, file, 0)
        file
      end

    refute File.exists?(file)

    assert {:ok, %BloomFilter{}, ^file, ^level} =
             SSTables.write(%SSTablesIterator{}, data, file, level)

    for i <- 1..5, j <- 1..10 do
      assert {:ok, {:value, "v-#{i * j}"}} == SSTables.read(file, i * j)
    end

    assert File.exists?(file)
  end

  test "fetch_ss_table_info/1 returns Bloom filter and corresponding level in SSTable", c do
    file = Path.join(c.tmp_dir, "foo")
    level = 0

    data =
      for n <- 1..10, reduce: %{} do
        acc ->
          Map.put(acc, n, "v-#{n}")
      end

    SSTables.write(%MemTableIterator{}, data, file, level)

    assert {:ok, %BloomFilter{}, ^level} = SSTables.fetch_ss_table_info(file)
  end

  test "fetch_ss_table_info/1 fails if file is not an SSTable", c do
    file = Path.join(c.tmp_dir, "foo")
    File.write(file, "not an SSTable")
    assert {:error, :not_an_ss_table} == SSTables.fetch_ss_table_info(file)
  end

  test "read/2 finds key in file, returning :error if not found", c do
    file = Path.join(c.tmp_dir, "foo")
    level = 0

    data =
      for n <- 1..10, reduce: %{} do
        acc ->
          Map.put(acc, n, "v-#{n}")
      end

    SSTables.write(%MemTableIterator{}, data, file, level)

    for n <- 1..10 do
      assert {:ok, {:value, "v-#{n}"}} == SSTables.read(file, n)
    end

    for n <- 11..20 do
      assert :error == SSTables.read(file, n)
    end
  end
end
