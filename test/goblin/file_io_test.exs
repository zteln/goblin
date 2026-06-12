defmodule Goblin.FileIOTest do
  use ExUnit.Case, async: true
  alias Goblin.FileIO

  @moduletag :tmp_dir

  setup ctx do
    path = Path.join(ctx.tmp_dir, "test.goblin")
    %{test_path: path}
  end

  describe "open/2, open!/2" do
    test "creates non-existing file in write mode only", ctx do
      refute File.exists?(ctx.test_path)

      assert {:error, :enoent} == FileIO.open(ctx.test_path)
      refute File.exists?(ctx.test_path)

      assert {:ok, _io} = FileIO.open(ctx.test_path, write?: true)
      assert File.exists?(ctx.test_path)
    end

    test "can open existing file for reading only", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      assert :ok == FileIO.close(io)

      assert {:ok, _io} = FileIO.open(ctx.test_path)
    end

    test "raises on failure", ctx do
      assert_raise Goblin.IOError, fn ->
        FileIO.open!(ctx.test_path)
      end
    end
  end

  describe "append/3, read/1" do
    test "writing round-trips", ctx do
      io = FileIO.open!(ctx.test_path, write?: true)
      term = :foo

      assert {:ok, size} = FileIO.append(io, term)
      assert size > 0
      FileIO.close(io)

      io = FileIO.open!(ctx.test_path)
      assert {:ok, term} == FileIO.read(io)
      assert :eof == FileIO.read(io)
    end

    test "cannot append in read-only mode", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      FileIO.close(io)

      {:ok, io} = FileIO.open(ctx.test_path)

      assert {:error, :ebadf} == FileIO.append(io, :foo)
    end

    test "can read sequentially", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      assert {:ok, _} = FileIO.append(io, :foo)
      assert {:ok, _} = FileIO.append(io, :bar)
      assert {:ok, _} = FileIO.append(io, :baz)
      FileIO.close(io)

      {:ok, io} = FileIO.open(ctx.test_path)
      assert {:ok, :foo} = FileIO.read(io)
      assert {:ok, :bar} = FileIO.read(io)
      assert {:ok, :baz} = FileIO.read(io)
      assert :eof = FileIO.read(io)
    end

    test "compress? = true compresses repetitive data", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      term = :binary.copy("x", 2 * 512)
      assert {:ok, size1} = FileIO.append(io, term)
      assert {:ok, size2} = FileIO.append(io, term, compress?: true)
      assert size1 > size2
      FileIO.close(io)

      {:ok, io} = FileIO.open(ctx.test_path)
      assert {:ok, term} == FileIO.read(io)
      assert {:ok, term} == FileIO.read(io)
      assert :eof = FileIO.read(io)
    end

    test "returns size of written data", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      {:ok, size} = FileIO.append(io, :foo)
      assert :ok == FileIO.sync(io)
      assert size == FileIO.size_of(ctx.test_path)
    end
  end

  describe "read/1/2, read_footer/1" do
    test "can read via position", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      offset1 = :filelib.file_size(io.path)
      FileIO.append(io, :foo)
      offset2 = :filelib.file_size(io.path)
      FileIO.append(io, :bar)
      offset3 = :filelib.file_size(io.path)
      FileIO.append(io, :baz)

      assert {:ok, :foo} = FileIO.read(io, offset: offset1)
      assert {:ok, :bar} = FileIO.read(io, offset: offset2)
      assert {:ok, :baz} = FileIO.read(io, offset: offset3)
    end

    test "can read footer", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      FileIO.append(io, :foo, footer?: true)
      assert {:ok, :foo} == FileIO.read_footer(io)
    end
  end

  describe "stream/1" do
    test "returns stream over entire file", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      FileIO.append(io, :foo)
      FileIO.append(io, :bar)
      FileIO.append(io, :baz)
      FileIO.close(io)

      {:ok, io} = FileIO.open(ctx.test_path)

      assert [{:ok, :foo}, {:ok, :bar}, {:ok, :baz}] ==
               io
               |> FileIO.stream()
               |> Enum.to_list()
    end

    test "indicates if file is corrupt via trailing garbage", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      {:ok, valid_size} = FileIO.append(io, :foo)
      # append garbage
      File.write!(ctx.test_path, :binary.copy(<<0xFF>>, 123), [:append])
      FileIO.close(io)

      {:ok, io} = FileIO.open(ctx.test_path)

      assert [{:ok, :foo}, {:corrupt, valid_size}] ==
               io
               |> FileIO.stream()
               |> Enum.to_list()
    end

    test "indicates if a file is corrupt via invalid size", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      {:ok, valid_size} = FileIO.append(io, :foo)
      {:ok, _} = FileIO.append(io, :bar)
      # truncate from (valid_size + header_size + 1) in file
      assert :ok == FileIO.truncate(io, valid_size + 21)
      FileIO.close(io)

      {:ok, io} = FileIO.open(ctx.test_path)

      assert [{:ok, :foo}, {:corrupt, valid_size}] ==
               io
               |> FileIO.stream()
               |> Enum.to_list()
    end

    test "indicates if a file is corrupt via invalid crc", ctx do
      {:ok, io} = FileIO.open(ctx.test_path, write?: true)
      {:ok, valid_size1} = FileIO.append(io, :foo)
      {:ok, valid_size2} = FileIO.append(io, :bar)
      FileIO.close(io)
      # simulate corrupt payload
      # change last byte to "1"
      {:ok, file} = :file.open(ctx.test_path, [:raw, :read, :binary, :write])
      :file.pwrite(file, valid_size1 + valid_size2 - 1, "1")

      {:ok, io} = FileIO.open(ctx.test_path)

      assert [{:ok, :foo}, {:corrupt, valid_size1}] ==
               io
               |> FileIO.stream()
               |> Enum.to_list()
    end
  end
end
