defmodule Goblin.FlusherTest do
  use ExUnit.Case, async: true

  alias Goblin.Flusher

  @opts [
    max_sst_size: 100 * 1024,
    bf_fpp: 0.01,
    bf_bit_array_size: 100
  ]

  describe "new/1" do
    test "creates a flusher with empty queue and nil ref" do
      flusher = Flusher.new(@opts)

      assert flusher.ref == nil
      assert :queue.is_empty(flusher.queue)
      assert flusher.flush_opts == @opts
    end
  end

  describe "push/3" do
    test "returns {:flush, ...} when ref is nil" do
      flusher = Flusher.new(@opts)

      assert {:flush, :mem_table, :wal, flusher} =
               Flusher.push(flusher, :mem_table, :wal)

      assert :queue.is_empty(flusher.queue)
    end

    test "returns {:noop, ...} when ref is set" do
      flusher =
        Flusher.new(@opts)
        |> Flusher.set_ref(make_ref())

      assert {:noop, flusher} = Flusher.push(flusher, :mem_table, :wal)

      refute :queue.is_empty(flusher.queue)
    end
  end

  describe "pop/1" do
    test "returns {:noop, ...} on empty queue" do
      flusher = Flusher.new(@opts)

      assert {:noop, _flusher} = Flusher.pop(flusher)
    end

    test "dequeues the next flush job" do
      # Push with ref set so it queues without dequeuing
      flusher =
        Flusher.new(@opts)
        |> Flusher.set_ref(make_ref())

      {:noop, flusher} = Flusher.push(flusher, :mem_table, :wal)

      # Clear ref, then pop
      flusher = Flusher.set_ref(flusher, nil)

      assert {:flush, :mem_table, :wal, _flusher} = Flusher.pop(flusher)
    end

    test "drains multiple queued jobs in FIFO order" do
      flusher =
        Flusher.new(@opts)
        |> Flusher.set_ref(make_ref())

      {:noop, flusher} = Flusher.push(flusher, :mem1, :wal1)
      {:noop, flusher} = Flusher.push(flusher, :mem2, :wal2)
      {:noop, flusher} = Flusher.push(flusher, :mem3, :wal3)

      flusher = Flusher.set_ref(flusher, nil)

      assert {:flush, :mem1, :wal1, flusher} = Flusher.pop(flusher)

      # Simulate completing the flush by clearing ref
      flusher = Flusher.set_ref(flusher, nil)
      assert {:flush, :mem2, :wal2, flusher} = Flusher.pop(flusher)

      flusher = Flusher.set_ref(flusher, nil)
      assert {:flush, :mem3, :wal3, flusher} = Flusher.pop(flusher)

      flusher = Flusher.set_ref(flusher, nil)
      assert {:noop, _flusher} = Flusher.pop(flusher)
    end
  end

  describe "set_ref/2" do
    test "sets and clears the ref" do
      flusher = Flusher.new(@opts)
      ref = make_ref()

      flusher = Flusher.set_ref(flusher, ref)
      assert flusher.ref == ref

      flusher = Flusher.set_ref(flusher, nil)
      assert flusher.ref == nil
    end
  end
end
