defmodule Goblin.DiskTables.BinarySearchIterator do
  @moduledoc false

  defstruct [
    :keys,
    :max_seq,
    :file,
    :handler,
    :low,
    :high
  ]

  @spec new(Goblin.DiskTables.DiskTable.t(), [Goblin.db_key()], Goblin.seq_no()) ::
          Goblin.Iterable.t()
  def new(disk_table, keys, seq) do
    keys =
      keys
      |> Enum.uniq()
      |> Enum.sort()

    %__MODULE__{
      file: disk_table.file,
      low: 0,
      high: disk_table.no_blocks,
      keys: keys,
      max_seq: seq
    }
  end

  defimpl Goblin.Iterable do
    alias Goblin.DiskTables.{Handler, Encoder}

    def init(iterator) do
      handler = Handler.open!(iterator.file)
      %{iterator | handler: handler}
    end

    def next(%{keys: []}), do: :ok

    def next(iterator) do
      [next_key | keys] = iterator.keys

      case binary_search(
             iterator.handler,
             next_key,
             iterator.low,
             iterator.high,
             iterator.max_seq
           ) do
        {:ok, new_low, triple} ->
          iterator = %{iterator | keys: keys, low: new_low}
          {triple, iterator}

        {:ok, :not_found} ->
          iterator = %{iterator | keys: keys}
          next(iterator)

        error ->
          Handler.close(iterator.handler)
          raise "Iteration failed with error: #{inspect(error)}"
      end
    end

    def deinit(iterator) do
      Handler.close(iterator.handler)
    end

    defp binary_search(_handler, _key, low, high, _seq) when high < low do
      {:ok, :not_found}
    end

    defp binary_search(handler, key, low, high, seq) do
      mid = div(low + high, 2)

      case read_block(handler, mid) do
        {:ok, {^key, s, _v} = triple} when s < seq ->
          check_left_neighbour(handler, mid - 1, triple, seq)

        {:ok, {^key, s, _v} = triple} when s >= seq ->
          check_right_neighbour(handler, mid + 1, triple, seq)

        {:ok, {k, _s, _v}} when key < k ->
          binary_search(handler, key, low, mid - 1, seq)

        {:ok, {k, _s, _v}} when key > k ->
          binary_search(handler, key, mid + 1, high, seq)

        error ->
          error
      end
    end

    defp check_left_neighbour(_handler, block_no, triple, _seq) when block_no <= 0 do
      {:ok, block_no + 1, triple}
    end

    defp check_left_neighbour(handler, block_no, {key, _, _} = triple, seq) do
      case read_block(handler, block_no) do
        {:ok, {^key, s, _v} = neighbouring_triple} when s < seq ->
          check_left_neighbour(handler, block_no - 1, neighbouring_triple, seq)

        {:ok, _neighbouring_triple} ->
          {:ok, block_no + 1, triple}

        error ->
          error
      end
    end

    defp check_right_neighbour(handler, block_no, {key, _, _}, seq) do
      case read_block(handler, block_no) do
        {:ok, {^key, s, _v} = neighbouring_triple} when s >= seq ->
          check_right_neighbour(handler, block_no + 1, neighbouring_triple, seq)

        {:ok, {^key, _s, _v} = neighbouring_triple} ->
          {:ok, block_no + 1, neighbouring_triple}

        {:ok, _neighbouring_triple} ->
          {:ok, :not_found}

        {:error, :end_of_sst} ->
          {:ok, :not_found}

        error ->
          error
      end
    end

    defp read_block(handler, block_no) do
      position = (block_no - 1) * Encoder.sst_block_unit_size()
      position = max(0, position)

      with {:ok, sst_header_block} <- Handler.read(handler, position, Encoder.sst_header_size()),
           {:ok, no_blocks} <- Encoder.decode_sst_header_block(sst_header_block),
           {:ok, sst_block} <-
             Handler.read(handler, position, no_blocks * Encoder.sst_block_unit_size()) do
        Encoder.decode_sst_block(sst_block)
      else
        {:error, :invalid_sst_header} ->
          read_block(handler, block_no - 1)

        error ->
          error
      end
    end
  end
end
