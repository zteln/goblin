defmodule Goblin.WALTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.WAL

  @sync_interval 200
  @wal_name :wal_test
  @wal_file "wal.goblin"
  @registry_name WALTest.ProcessRegistry
  @moduletag :tmp_dir

  setup c do
    wal = start_wal(c.tmp_dir)
    %{wal: wal, wal_file: Path.join(c.tmp_dir, @wal_file), registry: @registry_name}
  end

  test "WAL starts with empty file", c do
    assert {:ok, [nil: []]} == WAL.recover(c.registry)
  end

  test "WAL recovers writes after restart", c do
    assert {:ok, [nil: []]} == WAL.recover(c.registry)
    assert :ok == WAL.append(c.registry, [{0, :put, :k, :v}])

    assert_eventually do
      assert {:ok, [nil: [{0, :put, :k, :v}]]} == WAL.recover(c.registry)
    end

    stop_wal()
    start_wal(c.tmp_dir)
    assert {:ok, [nil: [{0, :put, :k, :v}]]} == WAL.recover(c.registry)
  end

  test "WAL creates a new WAL file on rotate", c do
    assert :ok == WAL.append(c.registry, [{0, :put, :k, :v}])
    assert {:ok, rotated_file} = WAL.rotate(c.registry)
    assert File.exists?(rotated_file)
    assert File.exists?(c.wal_file)
    refute rotated_file == c.wal_file
    assert {:ok, [{^rotated_file, [{0, :put, :k, :v}]}, {nil, []}]} = WAL.recover(c.registry)
  end

  test "WAL recovers writes from rotated files", c do
    assert :ok == WAL.append(c.registry, [{0, :put, :k, :v}])
    assert {:ok, rotated_file} = WAL.rotate(c.registry)
    assert :ok == WAL.append(c.registry, [{1, :put, :k, :v}])

    assert_eventually do
      assert {:ok, [{^rotated_file, [{0, :put, :k, :v}]}, {nil, [{1, :put, :k, :v}]}]} =
               WAL.recover(c.registry)
    end
  end

  test "WAL finds previously rotated files on start", c do
    assert :ok == WAL.append(c.registry, [{0, :put, :k, :v}])
    assert {:ok, rotated_file} = WAL.rotate(c.registry)

    stop_wal()
    wal = start_wal(c.tmp_dir)

    assert %{rotated_files: [^rotated_file]} = :sys.get_state(wal)
  end

  test "append/2 appends writes sequentially", c do
    assert :ok == WAL.append(c.registry, [{0, :put, :k1, :v1}])
    assert :ok == WAL.append(c.registry, [{1, :put, :k2, :v2}])
    assert :ok == WAL.append(c.registry, [{2, :put, :k3, :v3}])

    assert_eventually do
      assert {:ok,
              [
                {nil,
                 [
                   {0, :put, :k1, :v1},
                   {1, :put, :k2, :v2},
                   {2, :put, :k3, :v3}
                 ]}
              ]} = WAL.recover(c.registry)
    end
  end

  test "append/2 appends batch writes in reverse order", c do
    assert :ok == WAL.append(c.registry, [{1, :put, :k2, :v2}, {0, :put, :k1, :v1}])

    assert_eventually do
      assert {:ok,
              [
                {nil,
                 [
                   {0, :put, :k1, :v1},
                   {1, :put, :k2, :v2}
                 ]}
              ]} = WAL.recover(c.registry)
    end
  end

  test "sync/1 syncs the WAL file", c do
    assert :ok == WAL.append(c.registry, [{0, :put, :k, :v}])
    assert {:ok, [{nil, []}]} = WAL.recover(c.registry)
    assert :ok == WAL.sync(c.registry)

    assert {:ok,
            [
              {nil,
               [
                 {0, :put, :k, :v}
               ]}
            ]} = WAL.recover(c.registry)
  end

  defp start_wal(dir) do
    start_supervised({
      Goblin.ProcessRegistry,
      name: @registry_name
    })

    start_link_supervised!(
      {
        WAL,
        db_dir: dir, wal_name: @wal_name, sync_interval: @sync_interval, registry: @registry_name
      },
      id: :wal_test_id
    )
  end

  defp stop_wal do
    stop_supervised(:wal_test_id)
  end
end
