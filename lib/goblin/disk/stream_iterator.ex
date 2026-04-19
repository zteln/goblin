defmodule Goblin.Disk.StreamIterator do
  @moduledoc false

  defstruct [
    :path,
    :io,
    :max_seq
  ]

  @spec new(Goblin.Disk.Table.t(), Goblin.seq_no() | nil) ::
          Goblin.Iterable.t()
  def new(table, seq \\ nil) do
    %__MODULE__{path: table.path, max_seq: seq}
  end

  defimpl Goblin.Iterable do
    alias Goblin.FileIO
    alias Goblin.Disk.Table

    def init(iterator) do
      io = FileIO.open!(iterator.path, block_size: Table.block_size())
      %{iterator | io: io}
    end

    def next(iterator) do
      %{max_seq: max_seq} = iterator

      case FileIO.read(iterator.io) do
        {:ok, {_k, s, _v} = triple} when s <= max_seq ->
          {triple, iterator}

        {:ok, {_k, s, _v}} when s > max_seq ->
          next(iterator)

        {:ok, _} ->
          :ok

        :eof ->
          :ok

        error ->
          FileIO.close(iterator.io)
          raise "iteration failed with error: #{inspect(error)}"
      end
    end

    def deinit(iterator), do: FileIO.close(iterator.io)
  end
end
