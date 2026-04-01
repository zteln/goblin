defmodule Goblin.Flusher do
  @moduledoc false

  alias Goblin.{
    MemTable,
    Iterator,
    DiskTable
  }

  defstruct [
    :opts,
    queue: :queue.new()
  ]

  @type t :: %__MODULE__{
          opts: keyword(),
          queue: :queue.queue({MemTable.t(), term()})
        }

  @spec new(keyword()) :: t()
  def new(opts) do
    opts =
      opts
      |> Keyword.take([:max_sst_size, :next_file_f])
      |> Keyword.put(:level_key, 0)
      |> Keyword.put(:compress?, false)

    %__MODULE__{opts: opts}
  end

  @spec enqueue(t(), MemTable.t()) :: t()
  def enqueue(flusher, mem_table) do
    queue = :queue.in(mem_table, flusher.queue)
    %{flusher | queue: queue}
  end

  @spec dequeue(t()) :: {:noop, t()} | {:flush, MemTable.t(), t()}
  def dequeue(flusher) do
    case :queue.out(flusher.queue) do
      {:empty, _queue} ->
        {:noop, flusher}

      {{:value, mem_table}, queue} ->
        {:flush, mem_table, %{flusher | queue: queue}}
    end
  end

  @spec flush(t(), MemTable.t()) ::
          {:ok, [DiskTable.t()], MemTable.t()} | {:error, term()}
  def flush(flusher, mem_table) do
    stream =
      Iterator.linear_stream(fn ->
        Goblin.Queryable.stream(mem_table, nil, nil, :infinity)
      end)

    with {:ok, disk_tables} <- DiskTable.into(stream, flusher.opts) do
      {:ok, disk_tables, mem_table}
    end
  end
end
