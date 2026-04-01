defmodule Goblin.FlusherTest do
  use ExUnit.Case, async: true

  alias Goblin.Flusher

  @opts [
    max_sst_size: 100 * 1024,
    bf_fpp: 0.01,
    bf_bit_array_size: 100
  ]

  describe "new/1" do
    test "creates a flusher with empty queue" do
      flusher = Flusher.new(@opts)

      assert :queue.is_empty(flusher.queue)
      assert flusher.opts[:level_key] == 0
      assert flusher.opts[:compress?] == false
    end
  end

  describe "enqueue/2" do
    test "adds a mem_table to the queue" do
      flusher =
        Flusher.new(@opts)
        |> Flusher.enqueue(:mem_table)

      refute :queue.is_empty(flusher.queue)
    end
  end

  describe "dequeue/1" do
    test "returns {:noop, ...} on empty queue" do
      flusher = Flusher.new(@opts)

      assert {:noop, _flusher} = Flusher.dequeue(flusher)
    end

    test "dequeues the next flush job" do
      flusher =
        Flusher.new(@opts)
        |> Flusher.enqueue(:mem_table)

      assert {:flush, :mem_table, flusher} = Flusher.dequeue(flusher)
      assert :queue.is_empty(flusher.queue)
    end

    test "drains multiple queued jobs in FIFO order" do
      flusher =
        Flusher.new(@opts)
        |> Flusher.enqueue(:mem1)
        |> Flusher.enqueue(:mem2)
        |> Flusher.enqueue(:mem3)

      assert {:flush, :mem1, flusher} = Flusher.dequeue(flusher)
      assert {:flush, :mem2, flusher} = Flusher.dequeue(flusher)
      assert {:flush, :mem3, flusher} = Flusher.dequeue(flusher)
      assert {:noop, _flusher} = Flusher.dequeue(flusher)
    end
  end
end
