defmodule SeaGoat.LockManager.LockTest do
  use ExUnit.Case, async: true
  alias SeaGoat.LockManager.Lock

  test "make/1 returns struct with type" do
    assert %{type: :type} = Lock.make(:type)
  end

  test "add/3 adds to locks keys" do
    lock = Lock.make(:type)
    assert %{keys: %{"a" => 1}} = Lock.add(lock, "a", 1)
  end

  test "pop/2 pops key from lock" do
    lock = Lock.make(:type) |> Lock.add("a", 1) |> Lock.add("b", 2)
    assert {1, lock} = Lock.pop(lock, "a")
    assert %Lock{keys: %{"b" => 2}, type: :type} == lock
    assert {2, nil} == Lock.pop(lock, "b")
  end
end
