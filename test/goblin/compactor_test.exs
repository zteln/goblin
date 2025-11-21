defmodule Goblin.CompactorTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.Compactor

  @moduletag :tmp_dir
  setup_db(key_limit: 10, level_limit: 1024)

  describe "put/3" do
    test "updates state", c do
      assert %{levels: %{} = levels} = :sys.get_state(c.compactor)
      assert map_size(levels) == 0

      assert :ok == Compactor.put(c.compactor, 0, "foo", 0, 1, {1, 2})

      assert %{
               levels: %{
                 0 => %{
                   entries: %{
                     "foo" => %{
                       id: "foo",
                       priority: 0,
                       size: 1,
                       key_range: {1, 2}
                     }
                   }
                 }
               }
             } = :sys.get_state(c.compactor)
    end

    test "exceeding level limit causes a compaction", c do
      file = Path.join(c.tmp_dir, "foo")
      fake_sst(file, [{1, 0, "v-1"}])
      %{size: size} = File.stat!(file)

      assert :ok == Compactor.put(c.compactor, 0, file, 0, size * 100, {1, 1})

      assert_eventually do
        assert %{levels: %{1 => %{entries: entries}}} = :sys.get_state(c.compactor)
        assert [%{priority: 0, size: ^size, key_range: {1, 1}}] = Map.values(entries)
      end
    end

    test "two ssts are eventually merged after compaction", c do
      file1 = Path.join(c.tmp_dir, "foo")
      fake_sst(file1, [{1, 0, "v-1"}])
      %{size: size1} = File.stat!(file1)
      file2 = Path.join(c.tmp_dir, "bar")
      fake_sst(file2, [{2, 1, "v-2"}])
      %{size: size2} = File.stat!(file2)

      assert :ok == Compactor.put(c.compactor, 0, file1, 0, size1, {1, 1})
      assert :ok == Compactor.put(c.compactor, 0, file2, 1, size2, {2, 2})

      assert_eventually do
        assert %{levels: %{1 => %{entries: entries}}} = :sys.get_state(c.compactor)
        assert [%{priority: 0, size: size, key_range: {1, 2}}] = Map.values(entries)
        assert size < size1 + size2
      end
    end

    test "files are only cleaned if there are no active readers", c do
      reader =
        spawn_link(fn ->
          Goblin.transaction(
            c.db,
            fn _tx ->
              receive do
                :cont -> :ok
              end
            end,
            read_only: true
          )
        end)

      file1 = Path.join(c.tmp_dir, "foo")
      fake_sst(file1, [{1, 0, "v-1"}])
      %{size: size1} = File.stat!(file1)

      file2 = Path.join(c.tmp_dir, "bar")
      fake_sst(file2, [{2, 1, "v-2"}])
      %{size: size2} = File.stat!(file2)

      assert :ok == Compactor.put(c.compactor, 0, file1, 0, size1, {1, 1})
      assert :ok == Compactor.put(c.compactor, 0, file2, 1, size2, {2, 2})

      assert_eventually do
        assert %{clean_ups: [{[^file2, ^file1], _, _}]} = :sys.get_state(c.compactor)
        assert File.exists?(file1)
        assert File.exists?(file2)
      end

      send(reader, :cont)

      assert_eventually do
        assert %{clean_ups: []} = :sys.get_state(c.compactor)
        refute File.exists?(file1)
        refute File.exists?(file2)
      end
    end
  end
end
