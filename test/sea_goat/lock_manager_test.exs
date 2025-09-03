defmodule SeaGoat.LockManagerTest do
  use ExUnit.Case, async: true
  alias SeaGoat.LockManager

  setup do
    pid = start_supervised!({LockManager, name: :lock_manager_test})
    %{lock_manager: pid}
  end

  describe "lock/3" do
    test "allows multiple locks on public lock", c do
      parent = self()
      ref1 = make_ref()
      ref2 = make_ref()

      for ref <- [ref1, ref2] do
        spawn(fn ->
          assert :ok == LockManager.lock(c.lock_manager, [:foo], :public)
          send(parent, {:done, ref})
        end)
      end

      assert_receive {:done, ^ref1}
      assert_receive {:done, ^ref2}
    end

    test "allows only one lock on private lock", c do
      parent = self()
      ref = make_ref()

      spawn(fn ->
        assert :ok == LockManager.lock(c.lock_manager, [:foo], :private)
        send(parent, {:done, ref})

        receive do
          _ -> :ok
        end
      end)

      assert_receive {:done, ^ref}

      ref = make_ref()

      spawn(fn ->
        assert {:error, :locked} == LockManager.lock(c.lock_manager, [:foo], :public)
        send(parent, {:done, ref})
      end)

      assert_receive {:done, ^ref}
    end

    test "public lock unlocks to private lock", c do
      parent = self()
      ref = make_ref()

      pid =
        spawn(fn ->
          assert :ok == LockManager.lock(c.lock_manager, [:foo], :public)
          send(parent, {:done, ref})

          receive do
            :end -> :ok
          end
        end)

      assert_receive {:done, ^ref}

      ref = make_ref()

      spawn(fn ->
        assert :ok == LockManager.lock(c.lock_manager, [:foo], :private)
        send(parent, {:done, ref})
      end)

      refute_receive {:done, ^ref}
      send(pid, :end)
      assert_receive {:done, ^ref}
    end

    test "multiple public resources returns even when locked", c do
      parent = self()
      refs = for _ <- 1..3, do: make_ref()
      resources = [[:foo], [:bar], [:foo, :bar]]

      for {ref, resource} <- Enum.zip(refs, resources) do
        spawn(fn ->
          assert :ok == LockManager.lock(c.lock_manager, resource, :public)
          send(parent, {:done, ref})

          receive do
            _ -> :ok
          end
        end)
      end

      for ref <- refs, do: assert_receive({:done, ^ref})
    end

    test "private lock on multiple resources returns when all are unlocked", c do
      parent = self()
      refs = for _ <- 1..2, do: make_ref()
      resources = [[:foo], [:bar]]

      pids =
        for {ref, resource} <- Enum.zip(refs, resources) do
          spawn(fn ->
            assert :ok == LockManager.lock(c.lock_manager, resource, :public)
            send(parent, {:done, ref})

            receive do
              :end -> :ok
            end
          end)
        end

      ref = make_ref()

      spawn(fn ->
        assert :ok == LockManager.lock(c.lock_manager, Enum.flat_map(resources, & &1), :private)
        send(parent, {:done, ref})
      end)

      refute_receive {:done, ^ref}
      for pid <- pids, do: send(pid, :end)
      assert_receive {:done, ^ref}
    end

    test "only accepts lock types :public and :private", c do
      assert {:error, :invalid_lock_type} == LockManager.lock(c.lock_manager, [:foo], :bar)
    end
  end

  describe "unlock/2" do
    test "unlocks locked resource", c do
      assert :ok == LockManager.lock(c.lock_manager, [:foo], :private)
      assert :ok == LockManager.unlock(c.lock_manager, :foo)
    end

    test "releases public lock to private lock", c do
      parent = self()
      ref = make_ref()

      pid =
        spawn(fn ->
          assert :ok == LockManager.lock(c.lock_manager, [:foo], :public)
          send(parent, {:done, ref})

          receive do
            :end ->
              assert :ok == LockManager.unlock(c.lock_manager, :foo)
          end
        end)

      assert_receive {:done, ^ref}

      ref = make_ref()

      spawn(fn ->
        assert :ok == LockManager.lock(c.lock_manager, [:foo], :private)
        assert :ok == LockManager.unlock(c.lock_manager, :foo)
        send(parent, {:done, ref})
      end)

      refute_receive {:done, ^ref}
      send(pid, :end)
      assert_receive {:done, ^ref}
    end
  end
end
