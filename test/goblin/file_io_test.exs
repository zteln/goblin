defmodule Goblin.PersistenceTest do
  use ExUnit.Case, async: true
  alias Goblin.Persistence

  @moduletag :tmp_dir

  setup ctx do
    path = Path.join(ctx.tmp_dir, "test.goblin")
    %{test_path: path}
  end

  describe "open/2, open!/2" do
    test "creates non-existing file in write mode only", ctx do
      refute File.exists?(ctx.test_path)

      assert {:error, :enoent} == Persistence.open(ctx.test_path)
      refute File.exists?(ctx.test_path)

      assert {:ok, _io} = Persistence.open(ctx.test_path, write?: true)
      assert File.exists?(ctx.test_path)
    end

    test "can open existing file for reading only", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      assert :ok == Persistence.close(io)

      assert {:ok, _io} = Persistence.open(ctx.test_path)
    end

    test "raises on failure", ctx do
      assert_raise Goblin.IOError, fn ->
        Persistence.open!(ctx.test_path)
      end
    end
  end

  describe "append/3, offset_read/3, seq_read/2" do
    test "writing round-trips", ctx do
      io = Persistence.open!(ctx.test_path, write?: true)
      term = :foo

      assert {:ok, size} = Persistence.append(io, term)
      assert size > 0
      Persistence.close(io)

      io = Persistence.open!(ctx.test_path)
      assert {:ok, term} == Persistence.seq_read(io)
      assert {:error, :eof} == Persistence.seq_read(io)
    end

    test "cannot append in read-only mode", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      Persistence.close(io)

      {:ok, io} = Persistence.open(ctx.test_path)

      assert {:error, :ebadf} == Persistence.append(io, :foo)
    end

    test "can read sequentially", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      assert {:ok, _} = Persistence.append(io, :foo)
      assert {:ok, _} = Persistence.append(io, :bar)
      assert {:ok, _} = Persistence.append(io, :baz)
      Persistence.close(io)

      {:ok, io} = Persistence.open(ctx.test_path)
      assert {:ok, :foo} = Persistence.seq_read(io)
      assert {:ok, :bar} = Persistence.seq_read(io)
      assert {:ok, :baz} = Persistence.seq_read(io)
      assert {:error, :eof} = Persistence.seq_read(io)
    end

    test "can read from offsets", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      size1 = 0
      assert {:ok, _} = Persistence.append(io, :foo)
      size2 = :filelib.file_size(ctx.test_path)
      assert {:ok, _} = Persistence.append(io, :bar)
      size3 = :filelib.file_size(ctx.test_path)
      assert {:ok, _} = Persistence.append(io, :baz)

      assert {:ok, :foo} = Persistence.offset_read(io, size1)
      assert {:ok, :bar} = Persistence.offset_read(io, size2)
      assert {:ok, :baz} = Persistence.offset_read(io, size3)
    end

    test "can read from offset with smaller read size", ctx do
      data = :binary.copy("x", 100)
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      assert {:ok, _} = Persistence.append(io, data)

      assert {:ok, data} == Persistence.offset_read(io, 0, read_size: 99)
    end

    test "fails to read if header is truncated", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      assert {:ok, _} = Persistence.append(io, :foo)
      :ok = Persistence.truncate(io, 7)

      assert {:error, :failed_to_read} == Persistence.offset_read(io, 0)
    end

    test "compress? = true compresses repetitive data", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      term = :binary.copy("x", 2 * 512)
      assert {:ok, size1} = Persistence.append(io, term)
      assert {:ok, size2} = Persistence.append(io, term, compress?: true)
      assert size1 > size2
      Persistence.close(io)

      {:ok, io} = Persistence.open(ctx.test_path)
      assert {:ok, term} == Persistence.seq_read(io)
      assert {:ok, term} == Persistence.seq_read(io)
      assert {:error, :eof} = Persistence.seq_read(io)
    end

    test "returns size of written data", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      {:ok, size} = Persistence.append(io, :foo)
      assert :ok == Persistence.sync(io)
      assert size == Persistence.size_of(ctx.test_path)
    end
  end

  describe "read_footer/1" do
    test "can read footer", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      Persistence.append(io, :foo, footer?: true)
      assert {:ok, :foo} == Persistence.read_footer(io)
    end
  end

  describe "stream/1" do
    test "returns stream over entire file", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      Persistence.append(io, :foo)
      Persistence.append(io, :bar)
      Persistence.append(io, :baz)
      Persistence.close(io)

      {:ok, io} = Persistence.open(ctx.test_path)

      assert [{:ok, :foo}, {:ok, :bar}, {:ok, :baz}] ==
               io
               |> Persistence.stream()
               |> Enum.to_list()
    end

    test "indicates if file is corrupt via trailing garbage", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      {:ok, valid_size} = Persistence.append(io, :foo)
      # append garbage
      File.write!(ctx.test_path, :binary.copy(<<0xFF>>, 123), [:append])
      Persistence.close(io)

      {:ok, io} = Persistence.open(ctx.test_path)

      assert [{:ok, :foo}, {:corrupt, valid_size}] ==
               io
               |> Persistence.stream()
               |> Enum.to_list()
    end

    test "indicates if a file is corrupt via invalid size", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      {:ok, valid_size} = Persistence.append(io, :foo)
      {:ok, _} = Persistence.append(io, :bar)
      # truncate from (valid_size + header_size + 1) in file
      assert :ok == Persistence.truncate(io, valid_size + 9)
      Persistence.close(io)

      {:ok, io} = Persistence.open(ctx.test_path)

      assert [{:ok, :foo}, {:corrupt, valid_size}] ==
               io
               |> Persistence.stream()
               |> Enum.to_list()
    end

    test "indicates if a file is corrupt via invalid crc", ctx do
      {:ok, io} = Persistence.open(ctx.test_path, write?: true)
      {:ok, valid_size1} = Persistence.append(io, :foo)
      {:ok, valid_size2} = Persistence.append(io, :bar)
      Persistence.close(io)
      # simulate corrupt payload
      # change last byte to "1"
      {:ok, file} = :file.open(ctx.test_path, [:raw, :read, :binary, :write])
      :file.pwrite(file, valid_size1 + valid_size2 - 1, "1")

      {:ok, io} = Persistence.open(ctx.test_path)

      assert [{:ok, :foo}, {:corrupt, valid_size1}] ==
               io
               |> Persistence.stream()
               |> Enum.to_list()
    end
  end
end
