defmodule Goblin.DiskTables.DiskTableTest do
  use ExUnit.Case, async: true
  use Mimic
  @moduletag :tmp_dir

  setup c do
    counter = :counters.new(1, [])

    next_file_f = fn ->
      count = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      file = Path.join(c.tmp_dir, "#{count}.goblin")
      {"#{file}.tmp", file}
    end

    %{next_file_f: next_file_f}
  end

  describe "write_new/3" do
    test "creates new disk table", c do
      opts = [
        level_key: 0,
        compress?: false,
        max_sst_size: 100 * 512,
        bf_fpp: 0.01,
        bf_bit_array_size: 100
      ]

      data =
        for n <- 1..100 do
          {n, n - 1, "v-#{n}"}
        end

      assert {:ok, [_disk_table]} =
               Goblin.DiskTables.DiskTable.write_new(data, c.next_file_f, opts)
    end

    test "splits into several disk tables when exceeding max_sst_size", c do
      opts = [
        level_key: 0,
        compress?: false,
        max_sst_size: 50 * 512,
        bf_fpp: 0.01,
        bf_bit_array_size: 100
      ]

      data =
        for n <- 1..100 do
          {n, n - 1, "v-#{n}"}
        end

      assert {:ok, [_disk_table1, _disk_table2]} =
               Goblin.DiskTables.DiskTable.write_new(data, c.next_file_f, opts)
    end

    test "does not create any disk tables if data is empty", c do
      opts = [
        level_key: 0,
        compress?: false,
        max_sst_size: 50 * 512,
        bf_fpp: 0.01,
        bf_bit_array_size: 100
      ]

      assert {:ok, []} = Goblin.DiskTables.DiskTable.write_new([], c.next_file_f, opts)
    end

    test "returns {:error, reason} when error occurs", c do
      Goblin.DiskTables.Handler
      |> expect(:write, fn _handler, _sst_block ->
        {:error, :failed_to_write_sst_block}
      end)

      opts = [
        level_key: 0,
        compress?: false,
        max_sst_size: 50 * 512,
        bf_fpp: 0.01,
        bf_bit_array_size: 100
      ]

      data =
        for n <- 1..100 do
          {n, n - 1, "v-#{n}"}
        end

      assert {:error, :failed_to_write_sst_block} =
               Goblin.DiskTables.DiskTable.write_new(data, c.next_file_f, opts)
    end
  end

  describe "parse/1" do
    setup c do
      opts = [
        level_key: 0,
        compress?: false,
        max_sst_size: 100 * 512,
        bf_fpp: 0.01,
        bf_bit_array_size: 100
      ]

      data =
        for n <- 1..100 do
          {n, n - 1, "v-#{n}"}
        end

      {:ok, [disk_table]} =
        Goblin.DiskTables.DiskTable.write_new(data, c.next_file_f, opts)

      %{target_disk_table: disk_table}
    end

    test "can parse disk table", c do
      %{target_disk_table: %{file: file} = target_disk_table} = c
      assert {:ok, ^target_disk_table} = Goblin.DiskTables.DiskTable.parse(file)
    end
  end
end
