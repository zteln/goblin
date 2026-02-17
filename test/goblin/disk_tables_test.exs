defmodule Goblin.DiskTablesTest do
  use ExUnit.Case
  use Goblin.TestHelper
  use Mimic
  import ExUnit.CaptureLog
  alias Goblin.{DiskTables, Manifest}

  @one_file_repo "#{File.cwd!()}/test/support/fixtures/one_file_repo"

  setup_db()

  @tag db_opts: [mem_limit: 5 * Goblin.DiskTables.Encoder.sst_block_unit_size()]
  test "new disk tables are queryable and persist across restarts", c do
    # check initial empty state
    assert %{disk_tables: []} = Manifest.snapshot(c.manifest, [:disk_tables])

    # trigger flush
    flush_data =
      Goblin.TestHelper.trigger_flush(c)
      |> Enum.with_index(fn {key, value}, seq -> {key, seq, value} end)
      |> Enum.sort_by(fn {key, seq, _value} -> {key, -seq} end)
      |> Goblin.TestHelper.uniq_by_value(fn {key, _seq, _value} -> key end)
      |> Enum.map(fn {key, _seq, value} -> {key, value} end)

    # check that flush is complete
    assert_eventually do
      assert %{disk_tables: [_disk_table]} = Manifest.snapshot(c.manifest, [:disk_tables])
    end

    # check that all data is accessible
    Enum.each(flush_data, fn {key, val} ->
      assert val == Goblin.get(c.db, key)
    end)

    stop_db(name: __MODULE__)
    %{db: db} = start_db(name: __MODULE__, data_dir: c.tmp_dir)

    # check that data is still accessible
    Enum.each(flush_data, fn {key, val} ->
      assert val == Goblin.get(db, key)
    end)
  end

  @tag db_opts: [
         flush_level_file_limit: 2,
         mem_limit: 5 * Goblin.DiskTables.Encoder.sst_block_unit_size()
       ]
  test "compacts level 0 (flush level) when file limit is exceeded", c do
    # check initial empty state
    assert %{disk_tables: []} = Manifest.snapshot(c.manifest, [:disk_tables])

    # write to disk
    Goblin.TestHelper.trigger_flush(c)

    disk_table =
      assert_eventually do
        assert %{disk_tables: [disk_table]} = Manifest.snapshot(c.manifest, [:disk_tables])
        disk_table
      end

    Goblin.TestHelper.trigger_flush(c)

    # check that there is only one new disk table that is different
    assert_eventually do
      assert %{disk_tables: [new_disk_table]} = Manifest.snapshot(c.manifest, [:disk_tables])
      refute new_disk_table == disk_table
    end
  end

  @tag db_opts: [flush_level_file_limit: 2, mem_limit: 10 * 1024]
  test "filters tombstones when compacting to deepest level", c do
    # put and remove key, ensuring tombstone value
    Goblin.put(c.db, :key, :val)
    Goblin.remove(c.db, :key)

    Goblin.TestHelper.trigger_flush(c, fn -> Stream.iterate(0, &(&1 + 1)) end)
    Goblin.TestHelper.trigger_flush(c, fn -> Stream.iterate(0, &(&1 + 1)) end)

    # wait until compaction is over
    disk_table =
      assert_eventually do
        refute Goblin.compacting?(c.db)

        assert %{disk_tables: [disk_table]} =
                 Manifest.snapshot(c.manifest, [:disk_tables])

        assert {:ok, %{level_key: 1} = disk_table} = DiskTables.DiskTable.parse(disk_table)
        disk_table
      end

    # enumerate data in disk table
    keys =
      Goblin.Iterator.linear_stream(fn ->
        Goblin.Queryable.stream(disk_table, nil, nil, 10000)
      end)
      |> Stream.map(fn {key, _seq, _val} -> key end)
      |> Enum.to_list()

    # ensure removed key is not among enumerated data
    refute :key in keys
  end

  @tag db_opts: [
         flush_level_file_limit: 2,
         mem_limit: 5 * Goblin.DiskTables.Encoder.sst_block_unit_size()
       ]
  test "stops when compaction fails, recovers on restart", %{disk_tables: disk_tables} = c do
    Process.monitor(disk_tables)
    Process.flag(:trap_exit, true)

    DiskTables.DiskTable
    |> expect(:write_new, fn _stream, _next_file_f, _opts ->
      {:error, :failed_to_create_new_disk_tables}
    end)

    DiskTables.DiskTable
    |> allow(self(), disk_tables)

    with_log(fn ->
      # trigger compaction from flush level
      Goblin.TestHelper.trigger_flush(c)
      Goblin.TestHelper.trigger_flush(c)
      # check that the server crashes
      assert_receive {:DOWN, _ref, :process, ^disk_tables, :failed_to_create_new_disk_tables}, 500
    end)

    # ensure that the server restarts and continues compaction
    assert_eventually do
      assert %{disk_tables: [disk_table]} = Manifest.snapshot(c.manifest, [:disk_tables])
      assert {:ok, %{level_key: 1}} = DiskTables.DiskTable.parse(disk_table)
    end
  end

  describe "migration" do
    setup :set_mimic_global
    setup :verify_on_exit!

    @tag start_db?: false
    test "automatically migrates on version mismatch", c do
      # trigger migration
      DiskTables.DiskTable
      |> stub(:parse, fn _ ->
        {:error, :invalid_magic}
      end)

      File.cp_r!(@one_file_repo, c.tmp_dir)

      {disk_table_name, output} =
        with_log(fn ->
          %{manifest: manifest} = start_db(name: __MODULE__, data_dir: c.tmp_dir)

          assert_eventually do
            assert %{disk_tables: [disk_table_name]} = Manifest.snapshot(manifest, [:disk_tables])
            disk_table_name
          end
        end)

      # check that migration was successful
      assert output =~ "Migrating #{disk_table_name} to newer version"
      assert output =~ "Migrated #{disk_table_name} to newer version"
    end
  end
end
