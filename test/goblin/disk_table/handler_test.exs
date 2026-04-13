defmodule Goblin.DiskTable.HandlerTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir

  describe "open!/2, open/2" do
    test "fails to open read-only handler if file does not exists", ctx do
      file = Path.join(ctx.tmp_dir, "non_existing_file")
      refute File.exists?(file)
      assert {:error, :enoent} == Goblin.DiskTable.Handler.open(file)

      assert_raise RuntimeError, fn ->
        Goblin.DiskTable.Handler.open!(file)
      end
    end

    test "creates file if write?: true option is provided", ctx do
      file = Path.join(ctx.tmp_dir, "new_file")
      refute File.exists?(file)
      assert {:ok, _handler} = Goblin.DiskTable.Handler.open(file, write?: true)
      assert File.exists?(file)
    end
  end

  describe "write/2" do
    test "can append to file", ctx do
      file = Path.join(ctx.tmp_dir, "new_file")
      {:ok, handler} = Goblin.DiskTable.Handler.open(file, write?: true)

      assert {:ok, handler} = Goblin.DiskTable.Handler.write(handler, "content")
      assert {:ok, "content"} == File.read(file)
      assert {:ok, _handler} = Goblin.DiskTable.Handler.write(handler, ", new_content")
      assert {:ok, "content, new_content"} == File.read(file)
    end

    test "fails if handler is not in append mode", ctx do
      file = Path.join(ctx.tmp_dir, "new_file")
      File.touch(file)
      {:ok, handler} = Goblin.DiskTable.Handler.open(file)

      assert {:error, :ebadf} = Goblin.DiskTable.Handler.write(handler, "content")
    end
  end

  describe "read_from_end/3, read/2/3" do
    setup ctx do
      file = Path.join(ctx.tmp_dir, "file")
      content = "file content"
      {:ok, handler} = Goblin.DiskTable.Handler.open(file, write?: true)
      {:ok, handler} = Goblin.DiskTable.Handler.write(handler, content)
      Goblin.DiskTable.Handler.sync(handler)
      Goblin.DiskTable.Handler.close(handler)
      %{disk_table_file: file, disk_table_content: content}
    end

    test "reads data from end", ctx do
      {:ok, handler} = Goblin.DiskTable.Handler.open(ctx.disk_table_file)

      assert {:ok, :binary.part(ctx.disk_table_content, byte_size(ctx.disk_table_content) - 4, 3)} ==
               Goblin.DiskTable.Handler.read_from_end(handler, 4, 3)

      assert {:error, :einval} == Goblin.DiskTable.Handler.read_from_end(handler, 15, 2)
    end

    test "reads data", ctx do
      {:ok, handler} = Goblin.DiskTable.Handler.open(ctx.disk_table_file)
      assert {:error, :eof} == Goblin.DiskTable.Handler.read(handler, 5)

      assert {:ok, :binary.part(ctx.disk_table_content, 2, 3)} ==
               Goblin.DiskTable.Handler.read(handler, 2, 3)
    end
  end

  describe "seq_read/2" do
    setup ctx do
      file = Path.join(ctx.tmp_dir, "file")
      content = "file content"
      {:ok, handler} = Goblin.DiskTable.Handler.open(file, write?: true)
      {:ok, handler} = Goblin.DiskTable.Handler.write(handler, content)
      Goblin.DiskTable.Handler.sync(handler)
      Goblin.DiskTable.Handler.close(handler)
      %{disk_table_file: file, disk_table_content: content}
    end

    test "reads sequentially from file start", ctx do
      {:ok, handler} = Goblin.DiskTable.Handler.open(ctx.disk_table_file, start?: true)

      assert {:ok, "file"} = Goblin.DiskTable.Handler.seq_read(handler, 4)
      assert {:ok, " con"} = Goblin.DiskTable.Handler.seq_read(handler, 4)
      assert {:ok, "tent"} = Goblin.DiskTable.Handler.seq_read(handler, 4)
    end

    test "returns eof when reading past end of file", ctx do
      {:ok, handler} = Goblin.DiskTable.Handler.open(ctx.disk_table_file, start?: true)

      assert {:ok, "file content"} = Goblin.DiskTable.Handler.seq_read(handler, 12)
      assert {:error, :eof} = Goblin.DiskTable.Handler.seq_read(handler, 1)
    end
  end
end
