defmodule Goblin.WALTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Goblin.WAL

  @moduletag :tmp_dir

  setup ctx do
    path = Path.join(ctx.tmp_dir, "test.wal")
    {:ok, wal} = WAL.open(path)
    %{path: path, wal: wal}
  end

  describe "open/1, delete/1, close/1" do
    test "creates new file", ctx do
      path = Path.join(ctx.tmp_dir, "test1.wal")
      refute File.exists?(path)
      assert {:ok, _wal} = WAL.open(path)
      assert File.exists?(path)
    end

    test "can close and delete underlying file", ctx do
      assert File.exists?(ctx.path)
      assert :ok == WAL.close(ctx.wal)
      assert File.exists?(ctx.path)
      assert :ok == WAL.delete(ctx.wal)
      refute File.exists?(ctx.path)
    end
  end

  describe "append/2, stream/1" do
    test "round-trips data", ctx do
      assert :ok == WAL.append(ctx.wal, [{1, 2, 3}, :foo, [4, 5]])
      assert [{1, 2, 3}, :foo, [4, 5]] == WAL.stream(ctx.wal) |> Enum.to_list()
    end

    test "data is durable", ctx do
      assert :ok == WAL.append(ctx.wal, [:foo])
      WAL.close(ctx.wal)
      {:ok, wal} = WAL.open(ctx.path)
      assert [:foo] == WAL.stream(wal) |> Enum.to_list()
    end

    test "recovers from trailing garbage", ctx do
      assert :ok == WAL.append(ctx.wal, [:foo, :bar, :baz])
      WAL.close(ctx.wal)

      valid_size = :filelib.file_size(ctx.path)
      File.write!(ctx.path, :binary.copy(<<0xFF>>, 512), [:append])
      {:ok, wal} = WAL.open(ctx.path)

      assert [:foo, :bar, :baz] == WAL.stream(wal) |> Enum.to_list()
      assert valid_size == :filelib.file_size(ctx.path)
    end

    test "recovers from mid-write truncations", ctx do
      assert :ok == WAL.append(ctx.wal, [:foo])
      survived_size = :filelib.file_size(ctx.path)
      assert :ok == WAL.append(ctx.wal, [:bar])

      WAL.close(ctx.wal)

      {:ok, f} = :file.open(ctx.path, [:read, :write, :raw, :binary])
      # survived_size + something less than block header (< 8 bytes)
      {:ok, _} = :file.position(f, survived_size + 2)
      :ok = :file.truncate(f)
      :file.close(f)

      {:ok, wal} = WAL.open(ctx.path)

      assert [:foo] == WAL.stream(wal) |> Enum.to_list()
      assert :filelib.file_size(ctx.path) == survived_size
    end
  end

  @tag :property_tests
  property "accepts any term as data", ctx do
    check all(data <- term()) do
      assert :ok == WAL.append(ctx.wal, [data])
      assert [data] == WAL.stream(ctx.wal) |> Stream.take(-1) |> Enum.to_list()
    end
  end
end
