defmodule Goblin.MemTables.WALTest do
  use ExUnit.Case, async: true
  alias Goblin.MemTables.WAL

  @moduletag :tmp_dir

  describe "open/1" do
    test "increments filename of next WAL", c do
      assert {:ok, wal} = WAL.open(__MODULE__, WAL.filename(c.tmp_dir))
      assert :ok == WAL.close(wal)
      assert {:ok, wal} = WAL.open(wal)
      assert WAL.filename(c.tmp_dir, 1) == wal.file
    end
  end

  describe "stream_log!/1" do
    test "can stream appended terms", c do
      terms = [{:put, 0, :key, :val}, {:remove, 1, :key}]

      assert {:ok, wal} = WAL.open(__MODULE__, WAL.filename(c.tmp_dir))
      assert :ok == WAL.append(wal, terms)

      assert terms == WAL.stream_log!(wal) |> Enum.to_list()
    end
  end
end
