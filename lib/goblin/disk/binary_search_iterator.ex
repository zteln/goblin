defmodule Goblin.Disk.BinarySearchIterator do
  @moduledoc false

  defstruct [
    :keys,
    :max_seq,
    :path,
    :io,
    :low,
    :high
  ]

  @spec new(Goblin.Disk.Table.t(), [Goblin.db_key()], Goblin.seq_no()) ::
          Goblin.Iterable.t()
  def new(table, keys, seq) do
    keys =
      keys
      |> Enum.sort(:desc)
      |> Enum.reduce([], fn
        key, [] -> [key]
        key1, [key2 | _] = acc when key1 == key2 -> acc
        key, acc -> [key | acc]
      end)

    %__MODULE__{
      path: table.path,
      low: 1,
      high: table.no_blocks,
      keys: keys,
      max_seq: seq
    }
  end

  defimpl Goblin.Iterable do
    alias Goblin.FileIO
    alias Goblin.Disk.Table

    def init(iterator) do
      io = FileIO.open!(iterator.path, block_size: Table.block_size())
      %{iterator | io: io}
    end

    def next(%{keys: []}), do: :ok

    def next(iterator) do
      [next_key | keys] = iterator.keys

      case binary_search(
             iterator.io,
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
          FileIO.close(iterator.io)
          raise "Iteration failed with error: #{inspect(error)}"
      end
    end

    def deinit(iterator), do: FileIO.close(iterator.io)

    defp binary_search(_io, _key, low, high, _seq) when high < low do
      {:ok, :not_found}
    end

    defp binary_search(io, key, low, high, seq) do
      mid = div(low + high, 2)
      pos = (mid - 1) * Table.block_size()

      case FileIO.pread(io, pos) do
        {:ok, {k, s, _v} = triple} when k == key and s < seq ->
          check_left_neighbour(io, mid - 1, triple, seq)

        {:ok, {k, s, _v} = triple} when k == key and s >= seq ->
          check_right_neighbour(io, mid + 1, triple, seq)

        {:ok, {k, _s, _v}} when key < k ->
          binary_search(io, key, low, mid - 1, seq)

        {:ok, {k, _s, _v}} when key > k ->
          binary_search(io, key, mid + 1, high, seq)

        error ->
          error
      end
    end

    defp check_left_neighbour(_io, block_no, triple, _seq) when block_no <= 0 do
      {:ok, block_no + 1, triple}
    end

    defp check_left_neighbour(io, block_no, {key, _, _} = triple, seq) do
      pos = (block_no - 1) * Table.block_size()

      case FileIO.pread(io, pos) do
        {:ok, {k, s, _v} = neighbouring_triple} when k == key and s < seq ->
          check_left_neighbour(io, block_no - 1, neighbouring_triple, seq)

        {:ok, _neighbouring_triple} ->
          {:ok, block_no + 1, triple}

        error ->
          error
      end
    end

    defp check_right_neighbour(io, block_no, {key, _, _}, seq) do
      pos = (block_no - 1) * Table.block_size()

      case FileIO.pread(io, pos) do
        {:ok, {k, s, _v} = neighbouring_triple} when k == key and s >= seq ->
          check_right_neighbour(io, block_no + 1, neighbouring_triple, seq)

        {:ok, {k, _s, _v} = neighbouring_triple} when k == key ->
          {:ok, block_no + 1, neighbouring_triple}

        {:ok, _neighbouring_triple} ->
          {:ok, :not_found}

        :eof ->
          {:ok, :not_found}

        error ->
          error
      end
    end
  end
end
