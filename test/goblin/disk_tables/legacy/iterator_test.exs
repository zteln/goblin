defmodule Goblin.DiskTables.Legacy.IteratorTest do
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

  test "new/1 returns correct level_key and whether the disk table is compressed", c do
    disk_table = gen_new_disk_table(c.next_file_f, level_key: 42, compress?: false)
    assert {:ok, 42, false, _iterator} = Goblin.DiskTables.Legacy.Iterator.new(disk_table.file)
    disk_table = gen_new_disk_table(c.next_file_f, level_key: 17, compress?: true)
    assert {:ok, 17, true, _iterator} = Goblin.DiskTables.Legacy.Iterator.new(disk_table.file)
  end

  test "new/1 returns error if magic is wrong", c do
    Goblin.DiskTables.Legacy.Encoder
    |> expect(:validate_magic_block, fn _magic_block ->
      {:error, :invalid_magic}
    end)

    disk_table = gen_new_disk_table(c.next_file_f, level_key: 42, compress?: false)
    assert {:error, :invalid_magic} = Goblin.DiskTables.Legacy.Iterator.new(disk_table.file)
  end

  test "is iterable", c do
    data =
      for n <- 1..100 do
        {n, n - 1, "v-#{n}"}
      end

    disk_table = gen_new_disk_table(c.next_file_f, level_key: 42, compress?: false)
    assert {:ok, _, _, iterator} = Goblin.DiskTables.Legacy.Iterator.new(disk_table.file)

    Enum.reduce_while(data, Goblin.Iterable.init(iterator), fn triple, iterator ->
      case Goblin.Iterable.next(iterator) do
        :ok ->
          {:halt, :ok}

        {iterated_triple, iterator} ->
          assert iterated_triple == triple
          {:cont, iterator}
      end
    end)
  end

  defp gen_new_disk_table(data \\ nil, next_file_f, opts) do
    opts =
      Keyword.merge(opts,
        max_sst_size: 100 * 512,
        bf_fpp: 0.01
      )

    data =
      data ||
        for n <- 1..100 do
          {n, n - 1, "v-#{n}"}
        end

    {:ok, [disk_table]} =
      Goblin.DiskTables.DiskTable.write_new(data, next_file_f, opts)

    disk_table
  end
end
