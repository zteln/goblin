defmodule Goblin.DiskTable do
  @moduledoc false

  alias Goblin.{
    BloomFilter,
    FileIO
  }

  @disk_table_block_size 1024

  defstruct [
    :id,
    :level_key,
    :bloom_filter,
    :key_range,
    :seq_range,
    size: 0,
    no_blocks: 0
  ]

  def build(stream, opts) do
    opts = [
      block_size: @disk_table_block_size,
      max_size: opts[:max_size],
      filer: opts[:filer],
      compress?: opts[:compress?]
    ]

    stream
    |> FileIO.stream_write(%__MODULE__{}, &add_to_table(&2, &1, &3), opts)
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, table}, {:ok, tables} -> {:cont, {:ok, [table | tables]}}
      error, _acc -> {:halt, error}
    end)
  end

  def from_file(path) do
    io = FileIO.open!(path, block_size: @disk_table_block_size)

    try do
      case FileIO.pread(io, :eof) do
        {:ok, %__MODULE__{} = table} -> {:ok, table}
        {:ok, _} -> {:error, :invalid_disk_table}
        error -> error
      end
    after
      FileIO.close(io)
    end
  end

  def has_key?(dt, key) do
    within_min_max?(dt, key) and bloom_filter_member?(dt, key)
  end

  def search(dt, keys, seq) do
    Stream.resource(
      fn ->
        io = FileIO.open!(dt.id, block_size: @disk_table_block_size)
        low = 1
        high = dt.no_blocks
        {io, {low, high}, keys}
      end,
      fn
        {io, _, []} ->
          FileIO.close(io)
          {:halt, nil}

        {io, {low, high}, [next | keys]} ->
          case binary_search(io, next, low, high, seq) do
            {:ok, new_low, triple} ->
              {[triple], {io, {new_low, high}, keys}}

            {:ok, :not_found} ->
              {[], {io, {low, high}, keys}}

            error ->
              FileIO.close(io)
              raise "search failed with error: #{inspect(error)}"
          end
      end,
      fn
        {io, _, _} -> FileIO.close(io)
        _ -> :ok
      end
    )
  end

  def stream(dt, opts) do
    {min, max} = opts[:bounds] || {nil, nil}
    seq = opts[:seq] || :infinity

    if within_bounds?(dt, min, max) do
      Stream.resource(
        fn -> FileIO.open!(dt.id, block_size: @disk_table_block_size) end,
        fn io ->
          case FileIO.read(io) do
            {:ok, {_, s, _} = triple} when s <= seq ->
              {[triple], io}

            {:ok, _} ->
              {[], io}

            :eof ->
              {:halt, io}

            error ->
              FileIO.close(io)
              raise "iteration failed with error: #{inspect(error)}"
          end
        end,
        fn io -> FileIO.close(io) end
      )
    else
      []
    end
  end

  defp add_to_table(table, triple, size) do
    {key, seq, _val} = triple

    key_range =
      case table.key_range do
        nil -> {key, key}
        {min, _} -> {min, key}
      end

    seq_range =
      case table.seq_range do
        nil -> {seq, seq}
        {min, _} -> {min, seq}
      end

    bloom_filter = BloomFilter.put(table.bloom_filter, key)
    no_blocks = table.no_blocks + div(size, @disk_table_block_size)

    %{
      table
      | key_range: key_range,
        seq_range: seq_range,
        bloom_filter: bloom_filter,
        no_blocks: no_blocks,
        size: table.size + size
    }
  end

  defp binary_search(_io, _key, low, high, _seq) when high < low do
    {:ok, :not_found}
  end

  defp binary_search(io, key, low, high, seq) do
    mid = div(low + high, 2)
    pos = (mid - 1) * @disk_table_block_size

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
    pos = (block_no - 1) * @disk_table_block_size

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
    pos = (block_no - 1) * @disk_table_block_size

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

  defp within_min_max?(%{key_range: {min, max}}, key),
    do: min <= key and key <= max

  defp bloom_filter_member?(dt, key),
    do: BloomFilter.member?(dt.bloom_filter, key)

  defp within_bounds?(_dt, nil, nil), do: true
  defp within_bounds?(%{key_range: {_, max}}, min, nil), do: min <= max
  defp within_bounds?(%{key_range: {min, _}}, nil, max), do: min <= max

  defp within_bounds?(%{key_range: {min1, max1}}, min2, max2),
    do: min1 <= max2 and min2 <= max1
end
