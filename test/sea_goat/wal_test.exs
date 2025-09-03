defmodule SeaGoat.WALTest do
  use ExUnit.Case, async: true
  alias SeaGoat.WAL

  @moduletag :tmp_dir

  setup c do
    pid = start_supervised!({WAL, name: :wal_test})
    log1 = Path.join(c.tmp_dir, "log1")
    log2 = Path.join(c.tmp_dir, "log2")
    %{wal: pid, log1: log1, log2: log2}
  end

  test "start_log/2 opens a log", c do
    assert :ok == WAL.start_log(c.wal, c.log1)
  end

  test "append/2 appends to current log", c do
    assert :ok == WAL.start_log(c.wal, c.log1)
    assert :ok == WAL.append(c.wal, "term1")
    assert :ok == WAL.append(c.wal, "term2")
    assert :ok == WAL.sync(c.wal)
    assert {:ok, ["term1", "term2"]} == WAL.replay(c.wal, c.log1)
  end

  test "rotate/4 closes current log and opens new log", c do
    assert :ok == WAL.start_log(c.wal, c.log1)
    assert :ok == WAL.append(c.wal, "term1")
    assert :ok == WAL.append(c.wal, "term2")
    assert :ok == WAL.rotate(c.wal, c.log2, "prepend", "append")
    assert {:ok, ["prepend", "term1", "term2", "append"]} == WAL.replay(c.wal, c.log1)
    assert :ok == WAL.append(c.wal, "term3")
    assert :ok == WAL.append(c.wal, "term4")
    assert :ok == WAL.sync(c.wal)
    assert {:ok, ["term3", "term4"]} == WAL.replay(c.wal, c.log2)
  end

  test "dump/3 dumps into separate log than current log", c do
    assert :ok == WAL.start_log(c.wal, c.log1)
    assert :ok == WAL.append(c.wal, "term1")
    assert :ok == WAL.append(c.wal, "term2")
    assert :ok == WAL.sync(c.wal)
    assert {:ok, ["term1", "term2"]} == WAL.replay(c.wal, c.log1)
    assert :ok == WAL.dump(c.wal, c.log2, ["term3", "term4"])
    assert {:ok, ["term3", "term4"]} == WAL.replay(c.wal, c.log2)
  end

  test "current_file/1 returns path to current log", c do
    assert :ok == WAL.start_log(c.wal, c.log1)
    assert c.log1 == WAL.current_file(c.wal)
    assert :ok == WAL.rotate(c.wal, c.log2, "", "")
    assert c.log2 == WAL.current_file(c.wal)
  end
end
