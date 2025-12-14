defmodule Goblin.WALTest do
  @moduledoc """
  Important qualities to test:

  - Non-trivial API:
    - `append/2`
    - `rotate/1`
    - `clean/2`
    - `recover/1`
  - Properties:
    - Recovers state from Manifest on start
  """
  use ExUnit.Case, async: true
  use TestHelper
  alias Goblin.WAL

  @moduletag :tmp_dir
  setup_db()

  describe "API" do
    test "append/2 appends to log and syncs", c do
      %{wal: {_, file}} = :sys.get_state(c.wal)
      %{size: size1} = File.stat!(file)

      assert :ok == WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])

      %{size: size2} = File.stat!(file)
      assert size2 > size1
    end

    test "rotate/1 generates a new WAL", c do
      no_files = length(File.ls!(c.tmp_dir))
      WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])

      assert {:ok, rotation, _current} = WAL.rotate(c.wal)

      WAL.append(c.wal, [{:put, 2, 2, 2}])

      assert no_files + 1 == length(File.ls!(c.tmp_dir))

      assert {:ok, [{^rotation, [{:put, 0, 0, 0}, {:put, 1, 1, 1}]}, {nil, [{:put, 2, 2, 2}]}]} =
               WAL.recover(c.wal)
    end

    test "clean/2 removes log file from disk and state", c do
      {:ok, rotation, _current} = WAL.rotate(c.wal)
      no_files = length(File.ls!(c.tmp_dir))

      assert %{rotations: [_]} = :sys.get_state(c.wal)

      assert :ok == WAL.clean(c.wal, rotation)

      assert_eventually do
        assert %{rotations: []} = :sys.get_state(c.wal)
      end

      assert no_files - 1 == length(File.ls!(c.tmp_dir))
    end

    test "clean/2 waits until there are no active readers before cleaning", c do
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

    test "recover/1 returns all logs", c do
      assert {:ok, [{nil, []}]} == WAL.recover(c.wal)
      WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])
      assert {:ok, [{nil, [{:put, 0, 0, 0}, {:put, 1, 1, 1}]}]} == WAL.recover(c.wal)

      {:ok, rotation, current} = WAL.rotate(c.wal)
      Goblin.Manifest.log_rotation(c.manifest, rotation, current)

      assert {:ok, [{^rotation, [{:put, 0, 0, 0}, {:put, 1, 1, 1}]}, {nil, []}]} =
               WAL.recover(c.wal)

      WAL.append(c.wal, [{:put, 2, 2, 2}, {:put, 3, 3, 3}])

      assert {:ok,
              [
                {^rotation, [{:put, 0, 0, 0}, {:put, 1, 1, 1}]},
                {nil, [{:put, 2, 2, 2}, {:put, 3, 3, 3}]}
              ]} = WAL.recover(c.wal)

      stop_db(__MODULE__)
      %{wal: wal} = start_db(c.tmp_dir, name: __MODULE__)

      assert {:ok,
              [
                {^rotation, [{:put, 0, 0, 0}, {:put, 1, 1, 1}]},
                {nil, [{:put, 2, 2, 2}, {:put, 3, 3, 3}]}
              ]} = WAL.recover(wal)
    end
  end

  describe "Property" do
    test "recovers state from Manifest on restart", c do
      WAL.append(c.wal, [{:put, 0, 0, 0}, {:put, 1, 1, 1}])
      {:ok, rotation, current} = WAL.rotate(c.wal)

      wal_state = :sys.get_state(c.wal)

      Goblin.Manifest.log_rotation(c.manifest, rotation, current)

      stop_db(__MODULE__)
      %{wal: wal} = start_db(c.tmp_dir, name: __MODULE__)

      assert ^wal_state = :sys.get_state(wal)
    end
  end
end
