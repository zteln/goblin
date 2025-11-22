defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Compactor

  @moduletag :tmp_dir
  setup_db(key_limit: 10, level_limit: 1024)

  describe "put/3" do
    test "single entry in flush level is compacted into higher level", c do
      file = Path.join(c.tmp_dir, "foo")

      fake_sst(file, [
        {"k1", 0, "v0"},
        {"k1", 1, "v1"},
        {"k1", 2, "v2"},
        {"k2", 3, "w0"},
        {"k2", 4, "w1"},
        {"k3", 5, "u0"}
      ])

      %{size: size} = File.stat!(file)

      assert :ok == Compactor.put(c.compactor, 0, file, 0, size, {"k1", "k3"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [{"k1", 2, "v2"}, {"k2", 4, "w1"}, {"k3", 5, "u0"}] ==
                 Goblin.SSTs.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end

    test "multiple entries in flush level are compacted into higher level", c do
      file1 = Path.join(c.tmp_dir, "foo1")
      file2 = Path.join(c.tmp_dir, "foo2")

      fake_sst(file1, [
        {"k1", 0, "v0"},
        {"k1", 1, "v1"},
        {"k3", 2, "u0"},
        {"k3", 3, "u1"},
        {"k3", 4, "u2"}
      ])

      fake_sst(file2, [
        {"k1", 5, "v2"},
        {"k1", 6, "v3"},
        {"k2", 7, "w0"},
        {"k2", 8, "w1"}
      ])

      assert :ok == Compactor.put(c.compactor, 0, file1, 0, 512, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 0, file2, 5, 512, {"k1", "k2"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [
                 {"k1", 6, "v3"},
                 {"k2", 8, "w1"},
                 {"k3", 4, "u2"}
               ] = Goblin.SSTs.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end

    test "multiple entries in flush level are merged with entry in higher level", c do
      file1 = Path.join(c.tmp_dir, "foo1")
      file2 = Path.join(c.tmp_dir, "foo2")
      file3 = Path.join(c.tmp_dir, "foo3")

      fake_sst(file1, [
        {"k1", 0, "v0"},
        {"k1", 1, "v1"},
        {"k2", 2, "w0"},
        {"k3", 3, "u0"},
        {"k3", 4, "u1"},
        {"k3", 5, "u2"}
      ])

      fake_sst(file2, [
        {"k1", 7, "v2"},
        {"k1", 8, "v3"},
        {"k2", 9, "w2"},
        {"k2", 10, "w3"}
      ])

      fake_sst(file3, [
        {"k3", 11, "u3"},
        {"k3", 12, "u4"},
        {"k4", 13, "x0"}
      ])

      assert :ok == Compactor.put(c.compactor, 1, file1, 0, 512, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 0, file2, 7, 512, {"k1", "k2"})
      assert :ok == Compactor.put(c.compactor, 0, file3, 11, 512, {"k3", "k4"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [
                 {"k1", 8, "v3"},
                 {"k2", 10, "w3"},
                 {"k3", 12, "u4"},
                 {"k4", 13, "x0"}
               ] = Goblin.SSTs.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end

    test "SST in one level above flush level is merged with overlapping SSTs in level above",
         c do
      file1 = Path.join(c.tmp_dir, "foo1")
      file2 = Path.join(c.tmp_dir, "foo2")

      fake_sst(file1, [
        {"k1", 0, "v0"},
        {"k1", 1, "v1"},
        {"k2", 2, "w0"},
        {"k3", 3, "u0"},
        {"k3", 4, "u1"},
        {"k3", 5, "u2"}
      ])

      fake_sst(file2, [
        {"k1", 7, "v2"},
        {"k1", 8, "v3"},
        {"k2", 9, "w2"},
        {"k2", 10, "w3"}
      ])

      assert :ok == Compactor.put(c.compactor, 2, file1, 0, 512, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 1, file2, 7, 1024 * 10, {"k1", "k2"})

      assert_eventually do
        assert %{compacting: nil, levels: %{2 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [
                 {"k1", 8, "v3"},
                 {"k2", 10, "w3"},
                 {"k3", 5, "u2"}
               ] = Goblin.SSTs.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 1)
      end
    end

    test "SSTs with no overlapping keys are not merged", c do
      file1 = Path.join(c.tmp_dir, "foo1")
      file2 = Path.join(c.tmp_dir, "foo2")

      fake_sst(file1, [
        {"k1", 1, "v0"},
        {"k2", 2, "w0"},
        {"k3", 3, "u0"}
      ])

      fake_sst(file2, [
        {"k4", 4, "x0"},
        {"k5", 5, "y0"}
      ])

      assert :ok == Compactor.put(c.compactor, 2, file1, 0, 512, {"k1", "k3"})
      assert :ok == Compactor.put(c.compactor, 1, file2, 7, 1024 * 10, {"k4", "k5"})

      assert_eventually do
        assert %{compacting: nil, levels: %{2 => [entry1, entry2]} = levels} =
                 :sys.get_state(c.compactor)

        assert [
                 {"k4", 4, "x0"},
                 {"k5", 5, "y0"}
               ] = Goblin.SSTs.stream!(entry1.id) |> Enum.to_list()

        assert [
                 {"k1", 1, "v0"},
                 {"k2", 2, "w0"},
                 {"k3", 3, "u0"}
               ] = Goblin.SSTs.stream!(entry2.id) |> Enum.to_list()

        refute Map.has_key?(levels, 1)
      end
    end

    test "tombstones in SST are cleaned if compacting to new level", c do
      file = Path.join(c.tmp_dir, "foo")

      fake_sst(file, [
        {"k1", 0, "v0"},
        {"k1", 1, "v1"},
        {"k1", 2, :"$goblin_tombstone"},
        {"k2", 3, "w0"},
        {"k2", 4, "w1"},
        {"k3", 5, "u0"}
      ])

      %{size: size} = File.stat!(file)

      assert :ok == Compactor.put(c.compactor, 0, file, 0, size, {"k1", "k3"})

      assert_eventually do
        assert %{compacting: nil, levels: %{1 => [entry]} = levels} = :sys.get_state(c.compactor)

        assert [{"k2", 4, "w1"}, {"k3", 5, "u0"}] ==
                 Goblin.SSTs.stream!(entry.id) |> Enum.to_list()

        refute Map.has_key?(levels, 0)
      end
    end
  end
end
