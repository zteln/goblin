defmodule Goblin.DiskTables.StoreTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  @disk_table_opts [
    level_key: 0,
    compress?: false,
    max_sst_size: 100 * Goblin.DiskTables.Encoder.sst_block_unit_size(),
    bf_fpp: 0.01
  ]

  setup c do
    store = Goblin.DiskTables.Store.new(__MODULE__)

    counter = :counters.new(1, [])

    next_file_f = fn ->
      count = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      file = Path.join(c.tmp_dir, "#{count}.goblin")
      {"#{file}.tmp", file}
    end

    %{store: store, next_file_f: next_file_f}
  end

  test "can update and read disk tables", c do
    assert [] == Goblin.DiskTables.Store.select_within_key_range(c.store, 1)
    assert [] == Goblin.DiskTables.Store.select_within_key_range(c.store, 2)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, nil, nil)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, 1, nil)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, nil, 2)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, 1, 2)

    triple1 = {1, 0, :crypto.strong_rand_bytes(1024)}
    triple2 = {2, 1, :crypto.strong_rand_bytes(1024)}

    {:ok, [%{file: file1} = disk_table1]} =
      Goblin.DiskTables.DiskTable.write_new([triple1], c.next_file_f, @disk_table_opts)

    {:ok, [%{file: file2} = disk_table2]} =
      Goblin.DiskTables.DiskTable.write_new([triple2], c.next_file_f, @disk_table_opts)

    assert :ok == Goblin.DiskTables.Store.insert(c.store, disk_table1)
    assert :ok == Goblin.DiskTables.Store.insert(c.store, disk_table2)

    assert [disk_table1] == Goblin.DiskTables.Store.select_within_key_range(c.store, 1)
    assert [disk_table2] == Goblin.DiskTables.Store.select_within_key_range(c.store, 2)

    assert Enum.sort([file1, file2]) ==
             Goblin.DiskTables.Store.select_within_bounds(c.store, nil, nil)
             |> Enum.sort()

    assert Enum.sort([file1, file2]) ==
             Goblin.DiskTables.Store.select_within_bounds(c.store, 1, nil)
             |> Enum.sort()

    assert Enum.sort([file1, file2]) ==
             Goblin.DiskTables.Store.select_within_bounds(c.store, nil, 2)
             |> Enum.sort()

    assert Enum.sort([file1, file2]) ==
             Goblin.DiskTables.Store.select_within_bounds(c.store, 1, 2)
             |> Enum.sort()

    assert :ok == Goblin.DiskTables.Store.remove(c.store, file1)
    assert :ok == Goblin.DiskTables.Store.remove(c.store, file2)

    assert [] == Goblin.DiskTables.Store.select_within_key_range(c.store, 1)
    assert [] == Goblin.DiskTables.Store.select_within_key_range(c.store, 2)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, nil, nil)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, 1, nil)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, nil, 2)
    assert [] == Goblin.DiskTables.Store.select_within_bounds(c.store, 1, 2)
  end

  test "wait_until_ready/2 raises if not ready within timeout, returns :ok otherwise", c do
    assert_raise RuntimeError, fn ->
      Goblin.DiskTables.Store.wait_until_ready(c.store, 200)
    end

    assert :ok == Goblin.DiskTables.Store.set_ready(c.store)
    assert :ok == Goblin.DiskTables.Store.wait_until_ready(c.store, 200)
  end
end
