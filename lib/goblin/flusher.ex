defmodule Goblin.Flusher do
  @moduledoc false

  alias Goblin.{
    Mem,
    Iterator,
    Disk
  }

  defstruct [
    :opts,
    queue: :queue.new()
  ]

  @type t :: %__MODULE__{
          opts: keyword(),
          queue: :queue.queue({Mem.t(), term()})
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

  @spec enqueue(t(), Mem.t()) :: t()
  def enqueue(flusher, mt) do
    queue = :queue.in(mt, flusher.queue)
    %{flusher | queue: queue}
  end

  @spec dequeue(t()) :: {:noop, t()} | {:flush, Mem.t(), t()}
  def dequeue(flusher) do
    case :queue.out(flusher.queue) do
      {:empty, _queue} ->
        {:noop, flusher}

      {{:value, mt}, queue} ->
        {:flush, mt, %{flusher | queue: queue}}
    end
  end

  @spec flush(t(), Mem.t()) ::
          {:ok, [Disk.Table.t()], Mem.t()} | {:error, term()}
  def flush(flusher, mt) do
    stream =
      Iterator.linear_stream(fn ->
        Mem.Iterator.new(mt, nil)
      end)

    with {:ok, dts} <- Disk.into_table(stream, flusher.opts) do
      {:ok, dts, mt}
    end
  end
end
