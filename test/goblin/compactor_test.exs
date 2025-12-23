defmodule Goblin.CompactorTest do
  @moduledoc """
  Important qualities to test:

  - Non-trivial API:
    - `put/6` 
    - `is_compacting/1`
  - Properties:
    - Compacts from flush level when flush_level_file_limit is exceeded
    - Compacts from one level to another when level_base_size * level_size_multiplier is exceeded
    - Only removes merged SSTs if there are no active readers
  """
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Compactor

  defmodule FakeTask do
    def async(_sup, _f) do
      %{ref: make_ref()}
    end
  end

  @moduletag :tmp_dir
  setup_db()

  describe "API" do
    test "put/6 updates Compactor state", c do
      file = Path.join(c.tmp_dir, "foo")
      sst = fake_sst([{1, 0, 1}, {2, 1, 2}], file: file, level_key: 0)

      assert %{levels: %{} = levels} = :sys.get_state(c.compactor)
      assert map_size(levels) == 0
      assert :ok = Compactor.put(c.compactor, sst)
      assert %{levels: %{0 => [entry]} = levels} = :sys.get_state(c.compactor)
      assert map_size(levels) == 1
      assert %{id: ^file, priority: 0, size: _, key_range: {1, 2}} = entry
    end

    @tag db_opts: [task_mod: FakeTask, flush_level_file_limit: 2]
    test "is_compacting/1 returns true when compacting, false otherwise", c do
      file1 = Path.join(c.tmp_dir, "foo")
      file2 = Path.join(c.tmp_dir, "bar")
      sst1 = fake_sst([{1, 0, 1}, {2, 1, 2}], file: file1, level_key: 0)
      sst2 = fake_sst([{3, 2, 3}, {4, 3, 4}], file: file2, level_key: 0)

      refute Compactor.is_compacting(c.compactor)

      Compactor.put(c.compactor, sst1)
      Compactor.put(c.compactor, sst2)

      assert_eventually do
        assert Compactor.is_compacting(c.compactor)
      end
    end
  end

  describe "Property" do
    @tag db_opts: [flush_level_file_limit: 2]
    test "compacts when flush_level_file_limit is exceeded", c do
      Goblin.put(c.db, :k1, :v1)
      Goblin.flush_now(c.db)

      assert_eventually do
        assert %{levels: %{0 => [entry]}} = :sys.get_state(c.compactor)

        assert [{:k1, 0, :v1}] ==
                 Goblin.Iterator.k_merge_stream([Goblin.DiskTable.iterator(entry.id)])
                 |> Enum.to_list()
      end

      Goblin.put(c.db, :k2, :v2)
      Goblin.flush_now(c.db)

      assert_eventually do
        assert %{levels: %{1 => [compacted_entry]}} = :sys.get_state(c.compactor)

        assert [{:k1, 0, :v1}, {:k2, 1, :v2}] ==
                 Goblin.Iterator.k_merge_stream([Goblin.DiskTable.iterator(compacted_entry.id)])
                 |> Enum.to_list()
      end
    end

    test "compacts when level_base_size * level_size_multiplier is exceeded", c do
      file1 = Path.join(c.tmp_dir, "foo")
      file2 = Path.join(c.tmp_dir, "bar")
      sst1 = fake_sst([{:k1, 0, :v1}], file: file1, level_key: 1)
      sst2 = fake_sst([{:k2, 1, :v2}], file: file2, level_key: 1)

      Compactor.put(c.compactor, %{sst1 | size: 134_217_729})
      Compactor.put(c.compactor, %{sst2 | size: 134_217_729})

      assert_eventually do
        assert %{levels: %{2 => [_]}} = :sys.get_state(c.compactor)
      end
    end

    @tag db_opts: [flush_level_file_limit: 2]
    test "only removes merged SSTs if there are no active readers", c do
      parent = self()

      reader =
        spawn(fn ->
          Goblin.transaction(
            c.db,
            fn _tx ->
              send(parent, :ready)

              receive do
                :done -> :ok
              end
            end,
            read_only: true
          )
        end)

      assert_receive :ready

      file1 = Path.join(c.tmp_dir, "foo")
      file2 = Path.join(c.tmp_dir, "bar")
      sst1 = fake_sst([{:k1, 0, :v1}], file: file1, level_key: 0)
      sst2 = fake_sst([{:k2, 1, :v2}], file: file2, level_key: 0)

      Compactor.put(c.compactor, sst1)
      Compactor.put(c.compactor, sst2)

      assert_eventually do
        refute Goblin.is_compacting(c.db)
      end

      assert "foo" in File.ls!(c.tmp_dir)
      assert "bar" in File.ls!(c.tmp_dir)

      send(reader, :done)

      assert_eventually do
        refute "foo" in File.ls!(c.tmp_dir)
        refute "bar" in File.ls!(c.tmp_dir)
      end
    end
  end
end
