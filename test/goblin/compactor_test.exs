defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  use Mimic
  import ExUnit.CaptureLog

  setup_db()

  @tag db_opts: [flush_level_file_limit: 2]
  test "compacts when flush_level_file_limit is exceeded", c do
    data =
      for n <- 1..200 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table1, disk_table2]} =
      generate_disk_table(data,
        disk_tables_server: c.disk_tables,
        max_sst_size: 100 * 512
      )

    assert :ok == Goblin.Compactor.put(c.compactor, disk_table1)
    assert :ok == Goblin.Compactor.put(c.compactor, disk_table2)

    assert_eventually do
      assert %{levels: %{1 => [compacted_entry]}} = :sys.get_state(c.compactor)

      assert data ==
               [Goblin.DiskTables.iterator(compacted_entry.id)]
               |> Goblin.Iterator.k_merge_stream()
               |> Enum.to_list()
    end
  end

  @tag db_opts: [level_base_size: 10 * 1024 * 1024]
  test "compacts when level_base_size * level_size_multiplier is exceeded", c do
    data =
      for n <- 1..200 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table1, disk_table2]} =
      generate_disk_table(data,
        level_key: 1,
        disk_tables_server: c.disk_tables,
        max_sst_size: 100 * 512
      )

    assert :ok ==
             Goblin.Compactor.put(c.compactor, %{disk_table1 | size: div(10 * 1024 * 1024, 2)})

    assert :ok ==
             Goblin.Compactor.put(c.compactor, %{disk_table2 | size: div(10 * 1024 * 1024, 2)})

    assert_eventually do
      assert %{levels: %{2 => [_]}} = :sys.get_state(c.compactor)
    end
  end

  @tag db_opts: [flush_level_file_limit: 2]
  test "only removes merged SSTs if there are no active readers", c do
    parent = self()

    reader =
      spawn(fn ->
        Goblin.read(
          c.db,
          fn _tx ->
            send(parent, :ready)

            receive do
              :done -> :ok
            end
          end
        )
      end)

    assert_receive :ready

    data =
      for n <- 1..200 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table1, disk_table2]} =
      generate_disk_table(data,
        disk_tables_server: c.disk_tables,
        max_sst_size: 100 * 512
      )

    assert :ok == Goblin.Compactor.put(c.compactor, disk_table1)
    assert :ok == Goblin.Compactor.put(c.compactor, disk_table2)

    assert_eventually do
      refute Goblin.compacting?(c.db)
    end

    assert Path.basename(disk_table1.file) in File.ls!(c.tmp_dir)
    assert Path.basename(disk_table2.file) in File.ls!(c.tmp_dir)

    send(reader, :done)

    assert_eventually do
      refute disk_table1.file in File.ls!(c.tmp_dir)
      refute disk_table2.file in File.ls!(c.tmp_dir)
    end
  end

  @tag db_opts: [flush_level_file_limit: 2]
  test "exits if compacting process exits", %{compactor: compactor} = c do
    Process.monitor(compactor)
    Process.flag(:trap_exit, true)

    Goblin.DiskTables
    |> expect(:new, fn _disk_tables_server, _stream, _opts ->
      {:error, :failed_to_compact}
    end)

    Goblin.DiskTables
    |> allow(self(), compactor)

    data =
      for n <- 1..200 do
        {n, n - 1, "v-#{n}"}
      end

    {:ok, [disk_table1, disk_table2]} =
      generate_disk_table(data,
        disk_tables_server: c.disk_tables,
        max_sst_size: 100 * 512
      )

    {_result, _log} =
      with_log(fn ->
        assert :ok == Goblin.Compactor.put(c.compactor, disk_table1)
        assert :ok == Goblin.Compactor.put(c.compactor, disk_table2)

        assert_receive {:DOWN, _ref, :process, ^compactor, :failed_to_compact}
      end)
  end
end
