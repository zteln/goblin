defmodule Talon.WALTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Talon.WAL

  @wal_id :wal_test_id
  @sync_interval 200
  @wal_name :wal_test
  @wal_file "wal.talon"
  @moduletag :tmp_dir

  setup c do
    wal = start_wal(c.tmp_dir)
    %{wal: wal, wal_file: Path.join(c.tmp_dir, @wal_file)}
  end

  test "WAL starts with empty file", c do
    assert {:ok, [nil: []]} == WAL.recover(c.wal)
  end

  test "WAL recovers writes after restart", c do
    assert {:ok, [nil: []]} == WAL.recover(c.wal)
    assert :ok == WAL.append(c.wal, [{0, :put, :k, :v}])

    assert_eventually do
      assert {:ok, [nil: [{0, :put, :k, :v}]]} == WAL.recover(c.wal)
    end

    stop_wal()
    wal = start_wal(c.tmp_dir)
    assert {:ok, [nil: [{0, :put, :k, :v}]]} == WAL.recover(wal)
  end

  test "WAL creates a new WAL file on rotate", c do
    assert :ok == WAL.append(c.wal, [{0, :put, :k, :v}])
    assert {:ok, rotated_file} = WAL.rotate(c.wal)
    assert File.exists?(rotated_file)
    assert File.exists?(c.wal_file)
    refute rotated_file == c.wal_file
    assert {:ok, [{^rotated_file, [{0, :put, :k, :v}]}, {nil, []}]} = WAL.recover(c.wal)
  end

  test "WAL recovers writes from rotated files", c do
    assert :ok == WAL.append(c.wal, [{0, :put, :k, :v}])
    assert {:ok, rotated_file} = WAL.rotate(c.wal)
    assert :ok == WAL.append(c.wal, [{1, :put, :k, :v}])

    assert_eventually do
      assert {:ok, [{^rotated_file, [{0, :put, :k, :v}]}, {nil, [{1, :put, :k, :v}]}]} =
               WAL.recover(c.wal)
    end
  end

  test "WAL finds previously rotated files on start", c do
    assert :ok == WAL.append(c.wal, [{0, :put, :k, :v}])
    assert {:ok, rotated_file} = WAL.rotate(c.wal)

    stop_wal()
    wal = start_wal(c.tmp_dir)

    assert %{rotated_files: [^rotated_file]} = :sys.get_state(wal)
  end

  test "append/2 appends writes sequentially", c do
    assert :ok == WAL.append(c.wal, [{0, :put, :k1, :v1}])
    assert :ok == WAL.append(c.wal, [{1, :put, :k2, :v2}])
    assert :ok == WAL.append(c.wal, [{2, :put, :k3, :v3}])

    assert_eventually do
      assert {:ok,
              [
                {nil,
                 [
                   {0, :put, :k1, :v1},
                   {1, :put, :k2, :v2},
                   {2, :put, :k3, :v3}
                 ]}
              ]} = WAL.recover(c.wal)
    end
  end

  test "append/2 appends batch writes in reverse order", c do
    assert :ok == WAL.append(c.wal, [{1, :put, :k2, :v2}, {0, :put, :k1, :v1}])

    assert_eventually do
      assert {:ok,
              [
                {nil,
                 [
                   {0, :put, :k1, :v1},
                   {1, :put, :k2, :v2}
                 ]}
              ]} = WAL.recover(c.wal)
    end
  end

  test "sync/1 syncs the WAL file", c do
    assert :ok == WAL.append(c.wal, [{0, :put, :k, :v}])
    assert {:ok, [{nil, []}]} = WAL.recover(c.wal)
    assert :ok == WAL.sync(c.wal)

    assert {:ok,
            [
              {nil,
               [
                 {0, :put, :k, :v}
               ]}
            ]} = WAL.recover(c.wal)
  end

  defp start_wal(dir) do
    start_link_supervised!({WAL, db_dir: dir, wal_name: @wal_name, sync_interval: @sync_interval},
      id: @wal_id
    )
  end

  defp stop_wal, do: stop_supervised(@wal_id)
end
