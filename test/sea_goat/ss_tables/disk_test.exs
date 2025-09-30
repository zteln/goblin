defmodule SeaGoat.SSTables.DiskTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables.Disk

  @moduletag :tmp_dir

  test "open!/2 returns io and offset to a file", c do
    path = Path.join(c.tmp_dir, "foo")
    File.touch(path)
    assert %Disk{io: {:file_descriptor, :prim_file, _}, offset: 0} = Disk.open!(path)
  end

  test "open!/2 raises on error", c do
    path = Path.join(c.tmp_dir, "non-existing-file")
    assert_raise RuntimeError, fn -> Disk.open!(path) end
  end

  test "open/2 opens in read-only mode", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "bar")
    assert {:ok, %Disk{offset: 3} = disk} = Disk.open(path)
    assert {:ok, "bar"} = Disk.read(disk, 0, 3)
    assert {:error, :ebadf} == Disk.write(disk, "baz")
  end

  test "open/2 opens in read-only mode at start", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "bar")
    assert {:ok, disk} = Disk.open(path, start?: true)
    assert {:ok, "bar"} = Disk.read(disk, 3)
    assert {:error, :ebadf} == Disk.write(disk, "baz")
  end

  test "open/2 opens in append mode", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "bar")
    assert {:ok, disk} = Disk.open(path, write?: true)
    assert {:ok, "bar"} = Disk.read(disk, 0, 3)
    assert {:ok, disk} = Disk.write(disk, "baz")
    assert {:ok, "barbaz"} = Disk.read(disk, 0, 6)
  end

  test "write/2 writes to a file", c do
    path = Path.join(c.tmp_dir, "foo")
    File.touch(path)
    assert {:ok, %Disk{offset: 0} = disk} = Disk.open(path, write?: true)
    assert {:ok, %Disk{offset: 3}} = Disk.write(disk, "bar")
  end

  test "write/2 failes if not opened in write mode", c do
    path = Path.join(c.tmp_dir, "foo")
    File.touch(path)
    assert {:ok, %Disk{offset: 0} = disk} = Disk.open(path)
    assert {:error, :ebadf} = Disk.write(disk, "bar")
  end

  test "read_from_end/2 reads content from the end", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "foobarbaz")
    {:ok, disk} = Disk.open(path)
    assert {:ok, "baz"} == Disk.read_from_end(disk, 3, 3)
    assert {:ok, "bar"} == Disk.read_from_end(disk, 6, 3)
    assert {:ok, "foo"} == Disk.read_from_end(disk, 9, 3)
    assert {:error, :eof} == Disk.read_from_end(disk, 0, 3)
  end

  test "read/2 reads from set offset", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "foobarbaz")
    {:ok, disk} = Disk.open(path, start?: true)
    assert {:ok, "foo"} == Disk.read(disk, 3)
    assert {:ok, "foobar"} == Disk.read(disk, 6)
    assert {:ok, "foobarbaz"} == Disk.read(disk, 9)
  end

  test "read/3 reads from position", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "foobarbaz")
    {:ok, disk} = Disk.open(path)
    assert {:ok, "foo"} == Disk.read(disk, 0, 3)
    assert {:ok, "bar"} == Disk.read(disk, 3, 3)
    assert {:ok, "baz"} == Disk.read(disk, 6, 3)
    assert {:error, :eof} == Disk.read(disk, 9, 3)
  end

  test "advance_offset/2 adds to offset", c do
    path = Path.join(c.tmp_dir, "foo")
    File.write(path, "foobarbaz")
    assert {:ok, %Disk{offset: 0} = disk} = Disk.open(path, start?: true)
    assert %Disk{offset: 3} = Disk.advance_offset(disk, 3)
  end
end
