defmodule Goblin.WALTest do
  use ExUnit.Case, async: true
  use TestHelper
  use Mimic
  alias Goblin.WAL

  @moduletag :tmp_dir
  setup_db()

  test "can append and read logs", c do
    assert [{wal, log_stream}] = WAL.get_log_streams(c.wal)
    assert [] == Enum.to_list(log_stream)
    %{size: size1} = File.stat!(wal)

    assert :ok == WAL.append(c.wal, [{:put, 0, 0, 0}])

    assert [{wal, log_stream}] = WAL.get_log_streams(c.wal)
    assert [{:put, 0, 0, 0}] == Enum.to_list(log_stream)
    %{size: size2} = File.stat!(wal)
    assert size2 > size1
  end

  test "can rotate and clean log files", c do
    assert :ok == WAL.append(c.wal, [{:put, 0, 0, 0}])
    assert {:ok, rotation, current} = WAL.rotate(c.wal)
    assert :ok == WAL.append(c.wal, [{:put, 1, 1, 1}])

    assert [{^rotation, log_stream1}, {^current, log_stream2}] = WAL.get_log_streams(c.wal)

    assert [{:put, 0, 0, 0}] == Enum.to_list(log_stream1)
    assert [{:put, 1, 1, 1}] == Enum.to_list(log_stream2)

    assert :ok == WAL.clean(c.wal, rotation)

    assert_eventually do
      assert [{^current, log_stream}] = WAL.get_log_streams(c.wal)
      assert [{:put, 1, 1, 1}] == Enum.to_list(log_stream)
    end

    assert_raise RuntimeError, fn ->
      Enum.to_list(log_stream1)
    end
  end

  test "gets state from Manifest on start", c do
    assert :ok == WAL.append(c.wal, [{:put, 0, 0, 0}])
    assert {:ok, rotation, current} = WAL.rotate(c.wal)
    assert :ok == WAL.append(c.wal, [{:put, 1, 1, 1}])
    Goblin.Manifest.log_rotation(c.manifest, rotation, current)

    assert [{^rotation, log_stream1}, {^current, log_stream2}] = WAL.get_log_streams(c.wal)
    assert [{:put, 0, 0, 0}] == Enum.to_list(log_stream1)
    assert [{:put, 1, 1, 1}] == Enum.to_list(log_stream2)

    stop_db(__MODULE__)
    %{wal: wal} = start_db(c.tmp_dir, name: __MODULE__)

    assert [{^rotation, log_stream1}, {^current, log_stream2}] = WAL.get_log_streams(wal)
    assert [{:put, 0, 0, 0}] == Enum.to_list(log_stream1)
    assert [{:put, 1, 1, 1}] == Enum.to_list(log_stream2)
  end
end
