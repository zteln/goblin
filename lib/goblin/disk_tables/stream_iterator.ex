defmodule Goblin.DiskTables.StreamIterator do
  @moduledoc false

  defstruct [
    :file,
    :handler,
    :max_seq
  ]

  @spec new(Path.t(), Goblin.seq_no() | nil) :: Goblin.Iterable.t()
  def new(file, seq \\ nil) do
    %__MODULE__{file: file, max_seq: seq}
  end

  defimpl Goblin.Iterable do
    alias Goblin.DiskTables.{Handler, Encoder}

    def init(iterator) do
      handler = Handler.open!(iterator.file, start?: true)
      %{iterator | handler: handler}
    end

    def next(iterator) do
      %{max_seq: max_seq} = iterator

      case read_next_sst_block(iterator.handler) do
        {:ok, triple, handler} when is_nil(max_seq) ->
          {triple, %{iterator | handler: handler}}

        {:ok, {_k, s, _v} = triple, handler} when s <= max_seq ->
          {triple, %{iterator | handler: handler}}

        {:ok, {_k, s, _v}, handler} when s > max_seq ->
          next(%{iterator | handler: handler})

        {:error, :end_of_sst} ->
          :ok

        error ->
          Handler.close(iterator.handler)
          raise "iteration failed with error: #{inspect(error)}"
      end
    end

    def deinit(iterator), do: Handler.close(iterator.handler)

    defp read_next_sst_block(handler) do
      with {:ok, sst_header_block} <- Handler.read(handler, Encoder.sst_header_size()),
           {:ok, no_blocks} <- Encoder.decode_sst_header_block(sst_header_block),
           {:ok, sst_block} <- Handler.read(handler, no_blocks * Encoder.sst_block_unit_size()),
           {:ok, triple} <- Encoder.decode_sst_block(sst_block) do
        {:ok, triple, Handler.advance_offset(handler, no_blocks * Encoder.sst_block_unit_size())}
      end
    end
  end
end
