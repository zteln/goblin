defmodule Goblin.WALTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.WAL

  @moduletag :tmp_dir

  describe "start_link/1" do
    test "opens a log", c do
      assert 0 == length(File.ls!(c.tmp_dir))
      assert {:ok, _wal} = WAL.start_link(db_dir: c.tmp_dir, name: __MODULE__)
      assert 1 == length(File.ls!(c.tmp_dir))
    end

    test "fails to open log in no existing directory", c do
      Process.flag(:trap_exit, true)
      non_existing_dir = Path.join(c.tmp_dir, "foo")

      assert {:error, {:file_error, _, :enoent}} =
               WAL.start_link(db_dir: non_existing_dir, name: __MODULE__)
    end

    test "fails to open log with same name", c do
      Process.flag(:trap_exit, true)
      assert {:ok, _wal} = WAL.start_link(db_dir: c.tmp_dir, name: __MODULE__)

      assert {:error, {:already_started, _pid}} =
               WAL.start_link(db_dir: c.tmp_dir, name: __MODULE__)
    end
  end

  describe "append/2" do
    setup c, do: start_wal(c.tmp_dir)

    test "appends to log and syncs file", c do
      %{file: file} = :sys.get_state(c.wal)
      %{size: size1} = File.stat!(file)

      assert :ok == WAL.append(c.wal, [:foo, :bar])
      %{size: size2} = File.stat!(file)
      assert size2 > size1

      assert {:ok, [{nil, [:bar, :foo]}]} == WAL.recover(c.wal)

      stop_wal()
      %{wal: wal} = start_wal(c.tmp_dir)

      assert {:ok, [{nil, [:bar, :foo]}]} == WAL.recover(wal)
    end
  end

  describe "rotate/2" do
    setup c, do: start_wal(c.tmp_dir)

    test "rotates log and opens new", c do
      assert 1 == length(File.ls!(c.tmp_dir))
      WAL.append(c.wal, [:foo, :bar])

      assert {:ok, rotated_wal} = WAL.rotate(c.wal)

      WAL.append(c.wal, [:baz])

      assert 2 == length(File.ls!(c.tmp_dir))
      assert {:ok, [{^rotated_wal, [:bar, :foo]}, {nil, [:baz]}]} = WAL.recover(c.wal)
    end
  end

  describe "clean/2" do
    setup c, do: start_wal(c.tmp_dir)

    test "removes rotated wal from disk and state", c do
      {:ok, rotated_wal} = WAL.rotate(c.wal)
      assert 2 == length(File.ls!(c.tmp_dir))

      assert %{rotated_files: [_]} = :sys.get_state(c.wal)

      assert :ok == WAL.clean(c.wal, rotated_wal)

      assert %{rotated_files: []} = :sys.get_state(c.wal)

      assert 1 == length(File.ls!(c.tmp_dir))
    end

    test "fails if rotated wal does not exist", c do
      assert {:error, :enoent} == WAL.clean(c.wal, Path.join(c.tmp_dir, "foo"))
    end
  end

  describe "recover/2" do
    setup c, do: start_wal(c.tmp_dir)

    test "get past logs from disk", c do
      assert {:ok, [{nil, []}]} == WAL.recover(c.wal)
      WAL.append(c.wal, [:foo, :bar])
      assert {:ok, [{nil, [:bar, :foo]}]} == WAL.recover(c.wal)

      {:ok, rotated_wal} = WAL.rotate(c.wal)

      assert {:ok, [{^rotated_wal, [:bar, :foo]}, {nil, []}]} = WAL.recover(c.wal)
      WAL.append(c.wal, [:foo, :bar])
      assert {:ok, [{^rotated_wal, [:bar, :foo]}, {nil, [:bar, :foo]}]} = WAL.recover(c.wal)

      stop_wal()
      %{wal: wal} = start_wal(c.tmp_dir)

      assert {:ok, [{^rotated_wal, [:bar, :foo]}, {nil, [:bar, :foo]}]} = WAL.recover(wal)
    end
  end

  defp start_wal(dir) do
    wal =
      start_link_supervised!({WAL, db_dir: dir, name: __MODULE__},
        id: __MODULE__
      )

    %{wal: wal}
  end

  defp stop_wal do
    stop_supervised(__MODULE__)
  end
end
