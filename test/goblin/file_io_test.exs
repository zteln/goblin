defmodule Goblin.FileIOTest do
  use ExUnit.Case, async: true

  alias Goblin.FileIO

  @moduletag :tmp_dir

  defp file_path(ctx), do: Path.join(ctx.tmp_dir, "test.goblin")

  defp open_write(ctx, opts \\ []) do
    {:ok, io} = FileIO.open(file_path(ctx), Keyword.merge([write?: true], opts))
    io
  end

  defp open_read(ctx, opts \\ []) do
    {:ok, io} = FileIO.open(file_path(ctx), opts)
    io
  end

  describe "open/2" do
    test "opens a read handle on an existing file", ctx do
      io = open_write(ctx)
      :ok = FileIO.close(io)

      assert {:ok, %FileIO{} = file} = FileIO.open(file_path(ctx))
      assert file.path == file_path(ctx)
      assert file.block_size == 512
    end

    test "returns an error when the file is missing in read mode", ctx do
      assert {:error, :enoent} = FileIO.open(file_path(ctx))
    end

    test "creates the file in write mode", ctx do
      refute File.exists?(file_path(ctx))

      assert {:ok, %FileIO{}} = FileIO.open(file_path(ctx), write?: true)
      assert File.exists?(file_path(ctx))
    end

    test "honors a custom block_size", ctx do
      assert {:ok, %FileIO{block_size: 1024}} =
               FileIO.open(file_path(ctx), write?: true, block_size: 1024)
    end
  end

  describe "open!/2" do
    test "returns a FileIO struct when open succeeds", ctx do
      assert %FileIO{} = FileIO.open!(file_path(ctx), write?: true)
    end

    test "raises when the file cannot be opened", ctx do
      assert_raise RuntimeError, ~r/failed to open file/, fn ->
        FileIO.open!(file_path(ctx))
      end
    end
  end

  describe "append/3 and read/1" do
    test "round-trips a single term with block-aligned size", ctx do
      io = open_write(ctx)
      term = {:put, 0, :a, "v1"}

      assert {:ok, size} = FileIO.append(io, term)
      assert size > 0
      assert rem(size, io.block_size) == 0
      :ok = FileIO.close(io)

      io = open_read(ctx)
      assert {:ok, ^term} = FileIO.read(io)
      assert :eof = FileIO.read(io)
    end

    test "reads multiple appended terms in order", ctx do
      io = open_write(ctx)
      terms = [{:a, 1}, %{nested: %{k: "v"}}, [1, 2, 3]]

      for term <- terms do
        {:ok, _size} = FileIO.append(io, term)
      end

      :ok = FileIO.close(io)

      io = open_read(ctx)

      for term <- terms do
        assert {:ok, ^term} = FileIO.read(io)
      end

      assert :eof = FileIO.read(io)
    end

    test "round-trips a large binary", ctx do
      io = open_write(ctx)
      term = :crypto.strong_rand_bytes(10_000)

      {:ok, _size} = FileIO.append(io, term)
      :ok = FileIO.close(io)

      io = open_read(ctx)
      assert {:ok, ^term} = FileIO.read(io)
    end

    test "compress? round-trips and reduces on-disk size for repetitive data", ctx do
      term = String.duplicate("goblin ", 5_000)

      io = open_write(ctx)
      {:ok, plain_size} = FileIO.append(io, term)
      :ok = FileIO.close(io)

      plain_path = file_path(ctx)
      compressed_path = Path.join(ctx.tmp_dir, "compressed.goblin")

      {:ok, cio} = FileIO.open(compressed_path, write?: true)
      {:ok, compressed_size} = FileIO.append(cio, term, compress?: true)
      :ok = FileIO.close(cio)

      assert compressed_size < plain_size

      {:ok, rio} = FileIO.open(compressed_path)
      assert {:ok, ^term} = FileIO.read(rio)
      :ok = FileIO.close(rio)

      {:ok, pio} = FileIO.open(plain_path)
      assert {:ok, ^term} = FileIO.read(pio)
      :ok = FileIO.close(pio)
    end

    test "honors a custom block_size on round-trip", ctx do
      io = open_write(ctx, block_size: 256)
      term = {:block_aligned, "payload"}

      {:ok, size} = FileIO.append(io, term)
      assert rem(size, 256) == 0
      :ok = FileIO.close(io)

      io = open_read(ctx, block_size: 256)
      assert {:ok, ^term} = FileIO.read(io)
    end
  end

  describe "pread/2" do
    test "reads terms at recorded offsets", ctx do
      io = open_write(ctx)
      terms = [{:first, 1}, {:second, 2}, {:third, 3}]

      {_, offsets} =
        Enum.reduce(terms, {0, []}, fn term, {pos, acc} ->
          {:ok, size} = FileIO.append(io, term)
          {pos + size, [pos | acc]}
        end)

      offsets = Enum.reverse(offsets)
      :ok = FileIO.close(io)

      io = open_read(ctx)

      for {term, pos} <- Enum.zip(terms, offsets) do
        assert {:ok, ^term} = FileIO.pread(io, pos)
      end
    end

    test ":eof returns the last appended term", ctx do
      io = open_write(ctx)

      for term <- [{:a, 1}, {:b, 2}, {:last, 99}] do
        {:ok, _size} = FileIO.append(io, term)
      end

      :ok = FileIO.close(io)

      io = open_read(ctx)
      assert {:ok, {:last, 99}} = FileIO.pread(io, :eof)
    end

    test ":eof on an empty file returns :empty", ctx do
      io = open_write(ctx)
      :ok = FileIO.close(io)

      io = open_read(ctx)
      assert {:error, :empty} = FileIO.pread(io, :eof)
    end

    test "negative positions return :invalid_position", ctx do
      io = open_write(ctx)
      {:ok, _size} = FileIO.append(io, :ok)
      :ok = FileIO.close(io)

      io = open_read(ctx)
      assert {:error, :invalid_position} = FileIO.pread(io, -1)
    end

    test ":eof walks backwards across a trailing invalid header", ctx do
      io = open_write(ctx)
      term = {:valid, "payload"}
      {:ok, _size} = FileIO.append(io, term)
      :ok = FileIO.close(io)

      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(file_path(ctx), garbage, [:append])

      io = open_read(ctx)
      assert {:ok, ^term} = FileIO.pread(io, :eof)
    end
  end

  describe "read/1" do
    test "returns :eof on an empty file", ctx do
      io = open_write(ctx)
      :ok = FileIO.close(io)

      io = open_read(ctx)
      assert :eof = FileIO.read(io)
    end

    test "returns :invalid_header when the next block is corrupt", ctx do
      io = open_write(ctx)
      term = {:good, "value"}
      {:ok, _size} = FileIO.append(io, term)
      :ok = FileIO.close(io)

      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(file_path(ctx), garbage, [:append])

      io = open_read(ctx)
      assert {:ok, ^term} = FileIO.read(io)
      assert {:error, :invalid_header} = FileIO.read(io)
    end
  end

  describe "stream!/2" do
    test "streams appended entries in order and halts at EOF", ctx do
      io = open_write(ctx)

      entries_a = [{:put, 0, :a, "v1"}, {:put, 1, :b, "v2"}]
      entries_b = [{:remove, 2, :a}]

      {:ok, _} = FileIO.append(io, entries_a)
      {:ok, _} = FileIO.append(io, entries_b)
      :ok = FileIO.close(io)

      io = open_read(ctx)
      streamed = FileIO.stream!(io) |> Enum.to_list()
      assert streamed == entries_a ++ entries_b
    end

    test "truncate?: true trims trailing garbage", ctx do
      io = open_write(ctx)
      entries = [{:put, 0, :a, "v1"}]
      {:ok, valid_size} = FileIO.append(io, entries)
      :ok = FileIO.close(io)

      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(file_path(ctx), garbage, [:append])

      assert :filelib.file_size(file_path(ctx)) == valid_size + 512

      io = open_write(ctx)
      streamed = FileIO.stream!(io, truncate?: true) |> Enum.to_list()
      assert streamed == entries
      :ok = FileIO.close(io)

      assert :filelib.file_size(file_path(ctx)) == valid_size

      io = open_read(ctx)
      assert (FileIO.stream!(io) |> Enum.to_list()) == entries
    end

    test "leaves trailing garbage in place when truncate? is not set", ctx do
      io = open_write(ctx)
      entries = [{:put, 0, :a, "v1"}]
      {:ok, valid_size} = FileIO.append(io, entries)
      :ok = FileIO.close(io)

      garbage = :binary.copy(<<0xFF>>, 512)
      File.write!(file_path(ctx), garbage, [:append])

      total_size = valid_size + 512
      assert :filelib.file_size(file_path(ctx)) == total_size

      io = open_read(ctx)
      streamed = FileIO.stream!(io) |> Enum.to_list()
      assert streamed == entries
      :ok = FileIO.close(io)

      assert :filelib.file_size(file_path(ctx)) == total_size
    end
  end

  describe "close/1" do
    test "closes the handle", ctx do
      io = open_write(ctx)
      assert :ok = FileIO.close(io)
    end
  end

  describe "sync/1" do
    test "syncs a writable handle", ctx do
      io = open_write(ctx)
      {:ok, _size} = FileIO.append(io, {:put, 0, :a, "v1"})

      assert :ok = FileIO.sync(io)
    end
  end

  describe "rename/2" do
    test "renames a file on disk", ctx do
      from = file_path(ctx)
      to = Path.join(ctx.tmp_dir, "renamed.goblin")

      io = open_write(ctx)
      :ok = FileIO.close(io)

      assert :ok = FileIO.rename(from, to)
      refute File.exists?(from)
      assert File.exists?(to)
    end
  end

  describe "remove/1" do
    test "removes an existing file", ctx do
      io = open_write(ctx)
      :ok = FileIO.close(io)

      assert File.exists?(file_path(ctx))
      assert :ok = FileIO.remove(file_path(ctx))
      refute File.exists?(file_path(ctx))
    end

    test "returns an error when the file is missing", ctx do
      assert {:error, :enoent} = FileIO.remove(file_path(ctx))
    end
  end
end
