defmodule Goblin.MemTablesTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper
  use Mimic
  import ExUnit.CaptureLog
  alias Goblin.{MemTables, Manifest}

  @mem_tables __MODULE__.MemTables

  setup_db()

  test "writes are durable across restarts", c do
    # perform single write
    assert :ok == MemTables.write(c.mem_tables, 1, [{:put, 0, :key, :val}])
    assert 1 == MemTables.get_sequence(@mem_tables)
    assert :val == Goblin.get(c.db, :key)

    # restart database
    stop_db(name: __MODULE__)
    %{db: db} = start_db(data_dir: c.tmp_dir, name: __MODULE__)

    # check for previous state
    assert_eventually do
      assert 1 == MemTables.get_sequence(@mem_tables)
      assert :val == Goblin.get(db, :key)
    end
  end

  @tag db_opts: [mem_limit: 10 * Goblin.DiskTables.Encoder.sst_block_unit_size()]
  test "flushes to disk when :mem_limit is exceeded", c do
    # initial empty state
    assert %{disk_tables: []} == Manifest.snapshot(c.manifest, [:disk_tables])

    # write enough to trigger a flush
    data = Goblin.TestHelper.trigger_flush(c)

    # check that a disk table is eventually written
    assert_eventually do
      assert %{disk_tables: [_disk_table]} = Manifest.snapshot(c.manifest, [:disk_tables])
    end

    # check that data written is accessible
    data
    |> Enum.with_index(fn {key, value}, seq -> {key, seq, value} end)
    |> Enum.sort_by(fn {key, seq, _value} -> {key, -seq} end)
    |> Goblin.TestHelper.uniq_by_value(fn {key, _seq, _value} -> key end)
    |> Enum.map(fn {key, _seq, value} -> {key, value} end)
    |> Enum.each(fn {key, value} ->
      assert value == Goblin.get(c.db, key)
    end)
  end

  @tag db_opts: [mem_limit: Goblin.DiskTables.Encoder.sst_block_unit_size()]
  test "continues interrupted flush on restart", %{mem_tables: mem_tables} = c do
    Process.monitor(mem_tables)
    Process.flag(:trap_exit, true)

    # setup to make flush crash once
    Goblin.DiskTables
    |> expect(:new, fn _disk_tables, _stream, _opts ->
      {:error, :failed_to_create_new_disk_tables}
    end)

    Goblin.DiskTables
    |> allow(self(), mem_tables)

    with_log(fn ->
      # ensure mem_tables server crashes when flushing
      Goblin.TestHelper.trigger_flush(c)
      assert_receive {:DOWN, _ref, :process, ^mem_tables, :failed_to_create_new_disk_tables}
    end)

    # check that the mem_tables server restarts and eventually finishes interrupted flush
    assert_eventually do
      assert %{disk_tables: disk_tables} = Manifest.snapshot(c.manifest, [:disk_tables])
      assert length(disk_tables) > 0
    end
  end

  test "stops when WAL fails to append", %{mem_tables: mem_tables} do
    Process.monitor(mem_tables)
    Process.flag(:trap_exit, true)

    Goblin.MemTables.WAL
    |> expect(:append, fn _wal, _writes ->
      {:error, :failed_to_append_to_wal}
    end)

    Goblin.MemTables.WAL
    |> allow(self(), mem_tables)

    with_log(fn ->
      assert {:error, :failed_to_append_to_wal} ==
               MemTables.write(mem_tables, 1, [{:put, 1, :key, :val}])

      assert_receive {:DOWN, _ref, :process, ^mem_tables, :failed_to_append_to_wal}
    end)
  end
end
