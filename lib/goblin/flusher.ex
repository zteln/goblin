defmodule Goblin.Flusher do
  @moduledoc false

  alias Goblin.{
    MemTable,
    Iterator,
    DiskTable
  }

  defstruct [
    :ref,
    :flush_opts,
    queue: :queue.new()
  ]

  @type t :: %__MODULE__{
          ref: reference() | nil,
          flush_opts: keyword(),
          queue: :queue.queue({MemTable.t(), term()})
        }

  @doc "Creates a new flusher with the given flush options."
  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      flush_opts: opts
    }
  end

  @doc "Enqueues a mem table and WAL for flushing."
  @spec push(t(), MemTable.t(), term()) ::
          {:noop, t()} | {:flush, MemTable.t(), term(), t()}
  def push(flusher, mem_table, wal) do
    queue = :queue.in({mem_table, wal}, flusher.queue)

    %{flusher | queue: queue}
    |> next()
  end

  @doc "Dequeues the next pending flush job."
  @spec pop(t()) :: {:noop, t()} | {:flush, MemTable.t(), term(), t()}
  def pop(flusher) do
    next(flusher)
  end

  @doc "Sets or clears the active flush task reference."
  @spec set_ref(t(), reference() | nil) :: t()
  def set_ref(flusher, ref), do: %{flusher | ref: ref}

  @doc "Flushes a mem table to level 0 disk tables."
  @spec flush(t(), MemTable.t(), term()) ::
          {:ok, [DiskTable.t()], term()} | {:error, term()}
  def flush(flusher, mem_table, wal) do
    opts =
      [
        level_key: 0,
        compress?: false
      ] ++ flusher.flush_opts

    stream =
      Iterator.linear_stream(fn ->
        %MemTable.Iterator{mem_table: mem_table}
      end)

    with {:ok, disk_tables} <- DiskTable.into(stream, opts) do
      {:ok, disk_tables, wal}
    end
  end

  defp next(%{ref: nil} = flusher) do
    case :queue.out(flusher.queue) do
      {:empty, queue} ->
        {:noop, %{flusher | queue: queue}}

      {{:value, {mem_table, wal}}, queue} ->
        {:flush, mem_table, wal, %{flusher | queue: queue}}
    end
  end

  defp next(flusher), do: {:noop, flusher}
end
