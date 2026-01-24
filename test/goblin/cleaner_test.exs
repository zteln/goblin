defmodule Goblin.CleanerTest do
  use ExUnit.Case, async: true
  use TestHelper
  import ExUnit.CaptureLog

  @cleaner_table __MODULE__.Cleaner
  @disk_tables __MODULE__.DiskTables

  setup_db(
    mem_limit: 2 * 1024,
    bf_bit_array_size: 1000
  )

  test "only removes files when there are no transactions", c do
    file = Path.join(c.tmp_dir, "foo")
    File.touch(file)
    assert File.exists?(file)
    assert :ok == Goblin.Cleaner.clean(c.cleaner, [file])
    refute File.exists?(file)
  end

  test "removes files after transactions are done", c do
    file = Path.join(c.tmp_dir, "foo")
    File.touch(file)
    assert File.exists?(file)
    assert :ok == Goblin.Cleaner.inc(@cleaner_table)
    assert :ok == Goblin.Cleaner.clean(c.cleaner, [file])
    assert File.exists?(file)
    assert :ok == Goblin.Cleaner.deinc(@cleaner_table, c.cleaner)

    assert_eventually do
      refute File.exists?(file)
    end
  end

  @tag db_opts: [max_sst_size: 100 * Goblin.DiskTables.Encoder.sst_block_unit_size()]
  test "disk tables are removed from disk tables server", c do
    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    keys = Enum.map(data, &elem(&1, 0))

    Goblin.DiskTables.new(c.disk_tables, data, level_key: 1, compress?: false)

    assert [iterator] = Goblin.DiskTables.search_iterators(@disk_tables, keys, length(data))

    assert :ok == Goblin.Cleaner.clean(c.cleaner, [iterator.file], true)

    assert_eventually do
      assert [] = Goblin.DiskTables.search_iterators(@disk_tables, keys, length(data))
    end
  end

  test "stops server if removing a file fails", %{cleaner: cleaner} = c do
    Process.monitor(cleaner)
    Process.flag(:trap_exit, true)

    {_result, _log} =
      with_log(fn ->
        Goblin.Cleaner.clean(c.cleaner, [Path.join(c.tmp_dir, "foo")])
        assert_receive {:DOWN, _ref, :process, ^cleaner, :enoent}
      end)
  end
end
