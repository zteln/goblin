defmodule Goblin.DiskTablesTest do
  use ExUnit.Case
  use TestHelper
  use Mimic
  import ExUnit.CaptureIO

  setup :set_mimic_global
  setup :verify_on_exit!

  @one_file_repo "#{File.cwd!()}/test/support/fixtures/one_file_repo"

  setup_db(
    mem_limit: 2 * 1024,
    bf_bit_array_size: 1000
  )

  @disk_tables __MODULE__.DiskTables

  test "can put, remove and get disk tables", c do
    data =
      for n <- 1..10 do
        {n, n - 1, "v-#{n}"}
      end

    assert [] = Goblin.DiskTables.search_iterators(@disk_tables, [1, 2, 3], 10)
    assert [] = Goblin.DiskTables.stream_iterators(@disk_tables, 1, 10, 10)

    assert {:ok, [disk_table_filename]} =
             Goblin.DiskTables.new(c.disk_tables, data, level_key: 0, compress?: false)

    assert [_iterator] = Goblin.DiskTables.search_iterators(@disk_tables, [1, 2, 3], 10)
    assert [_iterator] = Goblin.DiskTables.stream_iterators(@disk_tables, 1, 10, 10)

    assert :ok == Goblin.DiskTables.remove(c.disk_tables, disk_table_filename)

    assert [] = Goblin.DiskTables.search_iterators(@disk_tables, [1, 2, 3], 10)
    assert [] = Goblin.DiskTables.stream_iterators(@disk_tables, 1, 10, 10)
  end

  test "recovers state on start", c do
    data =
      for n <- 1..10 do
        {n, n - 1, "v-#{n}"}
      end

    assert {:ok, disk_table_filenames} =
             Goblin.DiskTables.new(c.disk_tables, data, level_key: 0, compress?: false)

    Goblin.Manifest.log_compaction(c.manifest, [], disk_table_filenames)

    assert [search_iterator] = Goblin.DiskTables.search_iterators(@disk_tables, [1, 2, 3], 10)
    assert [stream_iterator] = Goblin.DiskTables.stream_iterators(@disk_tables, 1, 10, 10)

    stop_db(__MODULE__)
    start_db(c.tmp_dir, name: __MODULE__)

    assert_eventually do
      assert [^search_iterator] = Goblin.DiskTables.search_iterators(@disk_tables, [1, 2, 3], 10)
      assert [^stream_iterator] = Goblin.DiskTables.stream_iterators(@disk_tables, 1, 10, 10)
    end
  end

  test "populates Compactor with new disk tables", c do
    data =
      for n <- 1..10 do
        {n, n - 1, "v-#{n}"}
      end

    assert {:ok, [disk_table_filename]} =
             Goblin.DiskTables.new(c.disk_tables, data, level_key: 0, compress?: false)

    assert %{levels: %{0 => [%{id: ^disk_table_filename}]}} = :sys.get_state(c.compactor)
  end

  @tag start_db?: false
  test "automatically migrates on version mismatch", c do
    Goblin.DiskTables.DiskTable
    |> stub(:parse, fn _ ->
      {:error, :invalid_magic}
    end)

    File.cp_r!(@one_file_repo, c.tmp_dir)

    {disk_table_name, output} =
      with_io(fn ->
        %{manifest: manifest} = start_db(c.tmp_dir, name: __MODULE__)

        assert_eventually do
          assert [_] = Goblin.DiskTables.search_iterators(__MODULE__.DiskTables, [1], 100_000)
        end

        %{disk_tables: [disk_table_name]} = Goblin.Manifest.snapshot(manifest, [:disk_tables])
        disk_table_name
      end)

    assert output =~ "Migrating #{disk_table_name} to newer version"
    assert output =~ "Migrated #{disk_table_name} to newer version"
  end
end
