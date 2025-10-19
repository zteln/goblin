defmodule SeaGoatDB.RWLocks.LockTest do
  use ExUnit.Case, async: true
  alias SeaGoatDB.RWLocks.Lock

  test "lock/3 updates lock" do
    assert {:locked, %Lock{waiting: nil, current: [{:rlock, :lock_id_0}]} = lock} =
             Lock.lock(%Lock{}, :r, :lock_id_0)

    assert {:locked,
            %Lock{waiting: nil, current: [{:rlock, :lock_id_1}, {:rlock, :lock_id_0}]} = lock} =
             Lock.lock(lock, :r, :lock_id_1)

    assert {:waiting,
            %Lock{
              waiting: {:wlock, :lock_id_2},
              current: [{:rlock, :lock_id_1}, {:rlock, :lock_id_0}]
            } = lock} =
             Lock.lock(lock, :w, :lock_id_2)

    assert {:busy,
            %Lock{
              waiting: {:wlock, :lock_id_2},
              current: [{:rlock, :lock_id_1}, {:rlock, :lock_id_0}]
            }} =
             Lock.lock(lock, :w, :lock_id_3)
  end

  test "lock/3 does not let readers acquire lock when writer has it" do
    assert {:locked, %Lock{waiting: nil, current: [{:wlock, :lock_id_0}]} = lock} =
             Lock.lock(%Lock{}, :w, :lock_id_0)

    assert {:busy, lock} == Lock.lock(lock, :r, :lock_id_1)
  end

  test "unlock/2 releases to waiting" do
    {:locked, lock} = Lock.lock(%Lock{}, :r, :lock_id_0)
    {:waiting, lock} = Lock.lock(lock, :w, :lock_id_2)
    {:locked, lock} = Lock.lock(lock, :r, :lock_id_1)

    assert {:unlocked,
            %Lock{waiting: {:wlock, :lock_id_2}, current: [{:rlock, :lock_id_1}]} = lock} =
             Lock.unlock(lock, fn id -> id == :lock_id_0 end)

    assert {:released, :lock_id_2, %Lock{waiting: nil, current: [{:wlock, :lock_id_2}]}} =
             Lock.unlock(lock, fn id -> id == :lock_id_1 end)
  end
end
