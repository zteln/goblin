defmodule Goblin.ViewTest do
  use ExUnit.Case, async: true

  alias Goblin.View

  setup do
    %{view: View.new()}
  end

  defp fake_table(id, lk), do: %{id: id, level_key: lk}

  defp add(view, id, lk) do
    t = fake_table(id, lk)
    :ok = View.add_table(view, id, lk, t)
    t
  end

  describe "new/0 + acquire/2" do
    test "fresh view returns {-1, 0, _} from acquire/2", ctx do
      assert {-1, 0, epoch} = View.acquire(ctx.view, make_ref())
      assert is_integer(epoch)
    end

    test "successive acquire/2 calls return strictly increasing epochs", ctx do
      {_, _, e1} = View.acquire(ctx.view, make_ref())
      {_, _, e2} = View.acquire(ctx.view, make_ref())
      {_, _, e3} = View.acquire(ctx.view, make_ref())

      assert e1 < e2
      assert e2 < e3
    end

    test "after update_sequence, acquire/2 returns the new sequence", ctx do
      :ok = View.update_sequence(ctx.view, 42)

      assert {-1, 42, _} = View.acquire(ctx.view, make_ref())
    end

    test "after adding a level_key=2 table, acquire/2 returns that level", ctx do
      add(ctx.view, :t1, 2)

      assert {2, _, _} = View.acquire(ctx.view, make_ref())
    end
  end

  describe "add_table/4 + get_tables/4" do
    test "a table added before acquire is visible to that reader", ctx do
      t1 = add(ctx.view, :t1, 0)

      {_, _, epoch} = View.acquire(ctx.view, make_ref())

      assert View.get_tables(ctx.view, epoch) == [t1]
    end

    test "a table added after acquire is not visible to that reader", ctx do
      {_, _, epoch} = View.acquire(ctx.view, make_ref())

      _t1 = add(ctx.view, :t1, 0)

      assert View.get_tables(ctx.view, epoch) == []
    end

    test "a later reader sees a table added between acquires", ctx do
      {_, _, _} = View.acquire(ctx.view, make_ref())
      t1 = add(ctx.view, :t1, 0)
      {_, _, later_epoch} = View.acquire(ctx.view, make_ref())

      assert View.get_tables(ctx.view, later_epoch) == [t1]
    end

    test "level_key filter narrows the visible set", ctx do
      t1 = add(ctx.view, :t1, 0)
      t2 = add(ctx.view, :t2, 1)

      {_, _, epoch} = View.acquire(ctx.view, make_ref())

      assert View.get_tables(ctx.view, epoch, 0) == [t1]
      assert View.get_tables(ctx.view, epoch, 1) == [t2]
    end

    test "filter function narrows the visible set", ctx do
      t1 = add(ctx.view, :t1, 0)
      _t2 = add(ctx.view, :t2, 0)

      {_, _, epoch} = View.acquire(ctx.view, make_ref())

      assert View.get_tables(ctx.view, epoch, :_, &(&1.id == :t1)) == [t1]
    end
  end

  describe "soft_delete_table/2 + get_tables/4" do
    test "a reader acquired before the soft-delete still sees the table", ctx do
      t1 = add(ctx.view, :t1, 0)

      {_, _, epoch} = View.acquire(ctx.view, make_ref())

      :ok = View.soft_delete_table(ctx.view, :t1)

      assert View.get_tables(ctx.view, epoch) == [t1]
    end

    test "a reader acquired after the soft-delete does not see the table", ctx do
      add(ctx.view, :t1, 0)
      :ok = View.soft_delete_table(ctx.view, :t1)

      {_, _, epoch} = View.acquire(ctx.view, make_ref())

      assert View.get_tables(ctx.view, epoch) == []
    end

    test "soft-deleting a key that doesn't exist is a no-op", ctx do
      assert :ok = View.soft_delete_table(ctx.view, :nope)
    end
  end

  describe "sweep/1" do
    test "returns [] when nothing has been soft-deleted", ctx do
      add(ctx.view, :t1, 0)

      assert View.sweep(ctx.view) == []
    end

    test "returns and removes a soft-deleted table when no readers are active", ctx do
      t1 = add(ctx.view, :t1, 0)

      :ok = View.soft_delete_table(ctx.view, :t1)

      assert View.sweep(ctx.view) == [t1]

      {_, _, epoch} = View.acquire(ctx.view, make_ref())
      assert View.get_tables(ctx.view, epoch) == []
    end

    test "returns [] while a reader acquired before the soft-delete is active", ctx do
      add(ctx.view, :t1, 0)

      reader_key = make_ref()
      {_, _, _} = View.acquire(ctx.view, reader_key)

      :ok = View.soft_delete_table(ctx.view, :t1)

      assert View.sweep(ctx.view) == []
    end

    test "after the holding reader releases, sweep returns the table", ctx do
      t1 = add(ctx.view, :t1, 0)

      reader_key = make_ref()
      {_, _, _} = View.acquire(ctx.view, reader_key)
      :ok = View.soft_delete_table(ctx.view, :t1)

      assert View.sweep(ctx.view) == []

      :ok = View.release(ctx.view, reader_key)

      assert View.sweep(ctx.view) == [t1]
    end

    test "a reader acquired after the soft-delete does not protect the table", ctx do
      t1 = add(ctx.view, :t1, 0)

      :ok = View.soft_delete_table(ctx.view, :t1)

      {_, _, _} = View.acquire(ctx.view, make_ref())

      assert View.sweep(ctx.view) == [t1]
    end
  end

  describe "release/2" do
    test "release of a never-acquired key is a no-op", ctx do
      assert :ok = View.release(ctx.view, make_ref())
    end

    test "release allows a held table to be swept", ctx do
      t1 = add(ctx.view, :t1, 0)

      key = make_ref()
      {_, _, _} = View.acquire(ctx.view, key)
      :ok = View.soft_delete_table(ctx.view, :t1)

      :ok = View.release(ctx.view, key)

      assert View.sweep(ctx.view) == [t1]
    end
  end

  describe "update_sequence/2" do
    test "the updated sequence is observable through acquire/2", ctx do
      :ok = View.update_sequence(ctx.view, 5)
      :ok = View.update_sequence(ctx.view, 15)

      assert {_, 15, _} = View.acquire(ctx.view, make_ref())
    end

    test "preserves max_lk set by add_table/4", ctx do
      add(ctx.view, :t1, 3)
      :ok = View.update_sequence(ctx.view, 7)

      assert {3, 7, _} = View.acquire(ctx.view, make_ref())
    end
  end
end
