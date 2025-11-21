defmodule Goblin.WALTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.WAL

  @moduletag :tmp_dir
  setup_db()

  describe "on start" do
    test "creates a new WAL and logs to manifest", c do
      assert %{wal: {0, file}} = :sys.get_state(c.wal)
      assert String.ends_with?(file, ".0")
      assert %{wal: ^file} = Goblin.Manifest.get_version(c.manifest, [:wal])
    end

    test "recovers rotations and current WAL from manifest", c do
      assert {:ok, rotation, current} = WAL.rotate(c.wal)
      Goblin.Manifest.log_rotation(c.manifest, rotation, current)

      stop_db(__MODULE__)
      %{wal: wal} = start_db(c.tmp_dir, name: __MODULE__)

      assert %{wal: {1, ^current}, rotations: [{0, ^rotation}]} = :sys.get_state(wal)
    end
  end

  describe "append/2" do
    test "appends to log and syncs file", c do
      %{wal: {_, file}} = :sys.get_state(c.wal)
      %{size: size1} = File.stat!(file)

      assert :ok == WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])
      %{size: size2} = File.stat!(file)
      assert size2 > size1

      assert {:ok, [{nil, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]}]} == WAL.recover(c.wal)

      stop_db(__MODULE__)
      %{wal: wal} = start_db(c.tmp_dir, name: __MODULE__)

      assert {:ok, [{nil, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]}]} == WAL.recover(wal)
    end
  end

  describe "rotate/2" do
    test "rotates log and opens new", c do
      no_of_files = length(File.ls!(c.tmp_dir))
      WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])

      assert {:ok, rotation, _current} = WAL.rotate(c.wal)

      WAL.append(c.wal, [{:put, 2, 2, 2}])

      assert no_of_files + 1 == length(File.ls!(c.tmp_dir))

      assert {:ok, [{^rotation, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]}, {nil, [{:put, 2, 2, 2}]}]} =
               WAL.recover(c.wal)
    end
  end

  describe "clean/2" do
    test "removes rotated wal from disk and state", c do
      {:ok, rotation, _current} = WAL.rotate(c.wal)
      no_of_files = length(File.ls!(c.tmp_dir))

      assert %{rotations: [_]} = :sys.get_state(c.wal)

      assert :ok == WAL.clean(c.wal, rotation)

      assert_eventually do
        assert %{rotations: []} = :sys.get_state(c.wal)
      end

      assert no_of_files - 1 == length(File.ls!(c.tmp_dir))
    end

    test "waits until there are no active readers", c do
      parent = self()
      {:ok, rotation, _current} = WAL.rotate(c.wal)

      reader =
        spawn(fn ->
          Goblin.transaction(
            c.db,
            fn _tx ->
              send(parent, :ready)

              receive do
                :cont -> :ok
              end
            end,
            read_only: true
          )

          send(parent, :done)
        end)

      assert_receive :ready

      assert :ok == WAL.clean(c.wal, rotation)

      assert %{rotations: [{_, ^rotation}]} = :sys.get_state(c.wal)

      send(reader, :cont)

      assert_receive :done

      assert_eventually do
        assert %{rotations: []} = :sys.get_state(c.wal)
      end
    end
  end

  describe "recover/2" do
    test "get past logs from disk", c do
      assert {:ok, [{nil, []}]} == WAL.recover(c.wal)
      WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])
      assert {:ok, [{nil, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]}]} == WAL.recover(c.wal)

      {:ok, rotation, current} = WAL.rotate(c.wal)
      Goblin.Manifest.log_rotation(c.manifest, rotation, current)

      assert {:ok, [{^rotation, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]}, {nil, []}]} =
               WAL.recover(c.wal)

      WAL.append(c.wal, [{:put, 2, 2, 2}, {:put, 3, 3, 3}])

      assert {:ok,
              [
                {^rotation, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]},
                {nil, [{:put, 3, 3, 3}, {:put, 2, 2, 2}]}
              ]} = WAL.recover(c.wal)

      stop_db(__MODULE__)
      %{wal: wal} = start_db(c.tmp_dir, name: __MODULE__)

      assert {:ok,
              [
                {^rotation, [{:put, 1, 1, 1}, {:put, 0, 0, 0}]},
                {nil, [{:put, 3, 3, 3}, {:put, 2, 2, 2}]}
              ]} = WAL.recover(wal)
    end
  end
end
