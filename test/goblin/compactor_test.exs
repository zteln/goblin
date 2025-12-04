defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Compactor

  @moduletag :tmp_dir
  setup_db()

  describe "put/3" do
    test "default 4 entries in flush level are compacted into level above", c do
      file1 = Path.join(c.tmp_dir, "1")
      file2 = Path.join(c.tmp_dir, "2")
      file3 = Path.join(c.tmp_dir, "3")
      file4 = Path.join(c.tmp_dir, "4")

      fake_sst(file1, [
        {"k1", 0, "10"},
        {"k1", 1, "11"},
        {"k1", 2, "12"},
        {"k2", 3, "20"},
        {"k2", 4, "21"},
        {"k3", 5, "30"}
      ])

      fake_sst(file2, [
        {"k3", 6, "31"},
        {"k3", 7, "32"},
        {"k4", 8, "40"},
        {"k4", 9, "41"}
      ])

      fake_sst(file3, [
        {"k5", 10, "50"},
        {"k5", 11, "51"},
        {"k5", 12, "52"},
        {"k6", 13, "60"}
      ])

      fake_sst(file4, [
        {"k2", 14, "22"},
        {"k6", 15, "61"}
      ])

      %{size: size1} = File.stat!(file1)
      %{size: size2} = File.stat!(file2)
      %{size: size3} = File.stat!(file3)
      %{size: size4} = File.stat!(file4)

      assert :ok == Compactor.put(c.compactor, 0, file1, 0, size1, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 0, file2, 6, size2, {"k3", "k4"})
      assert :ok == Compactor.put(c.compactor, 0, file3, 10, size3, {"k5", "k6"})
      assert :ok == Compactor.put(c.compactor, 0, file4, 14, size4, {"k2", "k6"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [
                 {"k1", 2, "12"},
                 {"k2", 14, "22"},
                 {"k3", 7, "32"},
                 {"k4", 9, "41"},
                 {"k5", 12, "52"},
                 {"k6", 15, "61"}
               ] ==
                 Goblin.DiskTable.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end

    test "multiple entries in flush level are merged with entry in higher level", c do
      file0 = Path.join(c.tmp_dir, "0")
      file1 = Path.join(c.tmp_dir, "1")
      file2 = Path.join(c.tmp_dir, "2")
      file3 = Path.join(c.tmp_dir, "3")
      file4 = Path.join(c.tmp_dir, "4")

      fake_sst(file0, [
        {"k1", 0, "10"},
        {"k2", 1, "20"},
        {"k3", 2, "30"},
        {"k4", 3, "40"},
        {"k5", 4, "50"},
        {"k6", 5, "60"},
        {"k7", 6, "70"}
      ])

      fake_sst(file1, [
        {"k1", 7, "11"},
        {"k1", 8, "12"},
        {"k1", 9, "13"},
        {"k2", 10, "21"},
        {"k2", 11, "22"},
        {"k3", 12, "31"}
      ])

      fake_sst(file2, [
        {"k3", 13, "32"},
        {"k3", 14, "33"},
        {"k4", 15, "41"},
        {"k4", 16, "42"}
      ])

      fake_sst(file3, [
        {"k5", 17, "51"},
        {"k5", 18, "52"},
        {"k5", 19, "53"},
        {"k6", 20, "61"}
      ])

      fake_sst(file4, [
        {"k2", 21, "23"},
        {"k6", 22, "62"}
      ])

      %{size: size0} = File.stat!(file0)
      %{size: size1} = File.stat!(file1)
      %{size: size2} = File.stat!(file2)
      %{size: size3} = File.stat!(file3)
      %{size: size4} = File.stat!(file4)

      assert :ok == Compactor.put(c.compactor, 1, file0, 0, size0, {"k1", "k7"})
      assert :ok == Compactor.put(c.compactor, 0, file1, 7, size1, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 0, file2, 13, size2, {"k3", "k4"})
      assert :ok == Compactor.put(c.compactor, 0, file3, 17, size3, {"k5", "k6"})
      assert :ok == Compactor.put(c.compactor, 0, file4, 21, size4, {"k2", "k6"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [
                 {"k1", 9, "13"},
                 {"k2", 21, "23"},
                 {"k3", 14, "33"},
                 {"k4", 16, "42"},
                 {"k5", 19, "53"},
                 {"k6", 22, "62"},
                 {"k7", 6, "70"}
               ] = Goblin.DiskTable.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end

    test "tombstones in SST are cleaned if compacting to new level", c do
      file1 = Path.join(c.tmp_dir, "1")
      file2 = Path.join(c.tmp_dir, "2")
      file3 = Path.join(c.tmp_dir, "3")
      file4 = Path.join(c.tmp_dir, "4")

      fake_sst(file1, [
        {"k1", 0, "10"},
        {"k1", 1, "11"},
        {"k1", 2, :"$goblin_tombstone"},
        {"k2", 3, "20"},
        {"k2", 4, "21"},
        {"k3", 5, "30"}
      ])

      fake_sst(file2, [
        {"k3", 6, "31"},
        {"k3", 7, "32"},
        {"k4", 8, "40"},
        {"k4", 9, "41"}
      ])

      fake_sst(file3, [
        {"k5", 10, "50"},
        {"k5", 11, "51"},
        {"k5", 12, "52"},
        {"k6", 13, "60"}
      ])

      fake_sst(file4, [
        {"k2", 14, "22"},
        {"k6", 15, "61"}
      ])

      %{size: size1} = File.stat!(file1)
      %{size: size2} = File.stat!(file2)
      %{size: size3} = File.stat!(file3)
      %{size: size4} = File.stat!(file4)

      assert :ok == Compactor.put(c.compactor, 0, file1, 0, size1, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 0, file2, 6, size2, {"k3", "k4"})
      assert :ok == Compactor.put(c.compactor, 0, file3, 10, size3, {"k5", "k6"})
      assert :ok == Compactor.put(c.compactor, 0, file4, 14, size4, {"k2", "k6"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [
                 {"k2", 14, "22"},
                 {"k3", 7, "32"},
                 {"k4", 9, "41"},
                 {"k5", 12, "52"},
                 {"k6", 15, "61"}
               ] ==
                 Goblin.DiskTable.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end
  end
end
