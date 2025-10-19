defmodule SeaGoatDB.RWLocksTest do
  use ExUnit.Case, async: true
  alias SeaGoatDB.RWLocks

  setup do
    pid = start_supervised!(RWLocks)
    %{rw_locks: pid}
  end

  test "wlock/2 returns `:ok` when acquiring a lock", c do
    assert :ok == RWLocks.wlock(c.rw_locks, :resource0)
  end

  test "wlock/2 does not allow two waiting writers", c do
    parent = self()
    ref = make_ref()

    assert :ok == RWLocks.wlock(c.rw_locks, :resource0)

    spawn(fn ->
      assert {:error, :lock_in_use} == RWLocks.wlock(c.rw_locks, :resource0)
      send(parent, {:done, ref})
    end)

    receive do
      {:done, ^ref} -> :ok
    end
  end

  test "wlock/2 blocks caller until acquired", c do
    parent = self()
    ref = make_ref()

    assert :ok == RWLocks.rlock(c.rw_locks, :resource0)

    spawn(fn ->
      assert :ok == RWLocks.wlock(c.rw_locks, :resource0)
      send(parent, {:acquired, ref})
    end)

    refute_receive {:acquired, ^ref}

    assert :ok == RWLocks.unlock(c.rw_locks, :resource0)

    assert_receive {:acquired, ^ref}
  end

  test "rlock/2 is not blocked when writer is waiting", c do
    parent = self()

    assert :ok == RWLocks.rlock(c.rw_locks, :resource0)

    ref = make_ref()

    spawn(fn ->
      assert :ok == RWLocks.wlock(c.rw_locks, :resource0)
      send(parent, {:acquired, ref})
    end)

    refute_receive {:acquired, ^ref}

    ref = make_ref()

    spawn(fn ->
      assert :ok == RWLocks.rlock(c.rw_locks, :resource0)
      send(parent, {:acquired, ref})
    end)

    assert_receive {:acquired, ^ref}
  end

  test "writer acquires when reader process terminates", c do
    parent = self()
    ref = make_ref()

    reader =
      spawn(fn ->
        assert :ok == RWLocks.rlock(c.rw_locks, :resource0)
        send(parent, {:acquired, ref})

        receive do
          :term -> :ok
        end
      end)

    assert_receive {:acquired, ^ref}

    spawn(fn ->
      assert :ok == RWLocks.wlock(c.rw_locks, :resource0)
      send(parent, {:acquired, ref})
    end)

    refute_receive {:acquired, ^ref}

    send(reader, :term)

    assert_receive {:acquired, ^ref}
  end
end
