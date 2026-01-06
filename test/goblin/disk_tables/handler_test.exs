defmodule Goblin.DiskTables.HandlerTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir

  describe "open!/2, open/2" do
    test "fails to open read-only handler if file does not exists", c do
      file = Path.join(c.tmp_dir, "non_existing_file")
      refute File.exists?(file)
      assert {:error, :enoent} == Goblin.DiskTables.Handler.open(file)

      assert_raise RuntimeError, fn ->
        Goblin.DiskTables.Handler.open!(file)
      end
    end

    test "creates file if write?: true option is provided", c do
      file = Path.join(c.tmp_dir, "new_file")
      refute File.exists?(file)
      assert {:ok, _handler} = Goblin.DiskTables.Handler.open(file, write?: true)
      assert File.exists?(file)
    end
  end

  describe "write/2" do
    test "can append to file", c do
      file = Path.join(c.tmp_dir, "new_file")
      {:ok, handler} = Goblin.DiskTables.Handler.open(file, write?: true)

      assert {:ok, handler} = Goblin.DiskTables.Handler.write(handler, "content")
      assert {:ok, "content"} == File.read(file)
      assert {:ok, _handler} = Goblin.DiskTables.Handler.write(handler, ", new_content")
      assert {:ok, "content, new_content"} == File.read(file)
    end

    test "fails if handler is not in append mode", c do
      file = Path.join(c.tmp_dir, "new_file")
      File.touch(file)
      {:ok, handler} = Goblin.DiskTables.Handler.open(file)

      assert {:error, :ebadf} = Goblin.DiskTables.Handler.write(handler, "content")
    end
  end

  describe "read_from_end/3, read/2/3" do
    setup c do
      file = Path.join(c.tmp_dir, "file")
      content = "file content"
      {:ok, handler} = Goblin.DiskTables.Handler.open(file, write?: true)
      {:ok, handler} = Goblin.DiskTables.Handler.write(handler, content)
      Goblin.DiskTables.Handler.sync(handler)
      Goblin.DiskTables.Handler.close(handler)
      %{disk_table_file: file, disk_table_content: content}
    end

    test "reads data from end", c do
      {:ok, handler} = Goblin.DiskTables.Handler.open(c.disk_table_file)

      assert {:ok, :binary.part(c.disk_table_content, byte_size(c.disk_table_content) - 4, 3)} ==
               Goblin.DiskTables.Handler.read_from_end(handler, 4, 3)

      assert {:error, :einval} == Goblin.DiskTables.Handler.read_from_end(handler, 15, 2)
    end

    test "reads data", c do
      {:ok, handler} = Goblin.DiskTables.Handler.open(c.disk_table_file)
      assert {:error, :eof} == Goblin.DiskTables.Handler.read(handler, 5)

      assert {:ok, :binary.part(c.disk_table_content, 2, 3)} ==
               Goblin.DiskTables.Handler.read(handler, 2, 3)
    end
  end
end

