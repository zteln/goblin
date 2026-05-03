defmodule Goblin.Disk.BinarySearch do
  @moduledoc false

  alias Goblin.FileIO
  alias Goblin.Disk.Table

  defstruct [
    :keys,
    :max_seq,
    :io,
    :high,
    low: 1
  ]

  @type t :: %__MODULE__{}

  @spec new(Table.t(), FileIO.t(), list(term()), non_neg_integer()) :: t()
  def new(table, io, keys, seq) do
    %__MODULE__{io: io, keys: keys, max_seq: seq, high: table.no_blocks}
  end

  @spec next(t()) ::
          {:ok, t()} | {:ok, {term(), non_neg_integer(), term()}, t()} | {:error, term()}
  def next(%{keys: []} = bs), do: {:ok, bs}

  def next(bs) do
    [next_key | keys] = bs.keys

    case binary_search(
           bs.io,
           next_key,
           bs.low,
           bs.high,
           bs.max_seq
         ) do
      {:ok, new_low, triple} ->
        bs = %{bs | keys: keys, low: new_low}
        {:ok, triple, bs}

      {:ok, :not_found} ->
        bs = %{bs | keys: keys}
        next(bs)

      error ->
        error
    end
  end

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
