defmodule Goblin.DiskTable do
  @moduledoc false

  alias Goblin.{
    BloomFilter,
    FileIO,
    IOError
  }

  @index_interval 4096

  defstruct [
    :id,
    :level_key,
    :bloom_filter,
    :key_range,
    :seq_range,
    index: [],
    size: 0
  ]

  @type t :: %__MODULE__{
          id: Path.t(),
          level_key: non_neg_integer(),
          bloom_filter: BloomFilter.t(),
          key_range: {term(), term()},
          seq_range: {non_neg_integer(), non_neg_integer()},
          index: list(),
          size: non_neg_integer()
        }

  @spec build(Enumerable.t({term(), non_neg_integer(), term()}), keyword()) ::
          {:ok, list(t())} | {:error, term()}
  def build(stream, opts) do
    max_size = opts[:max_size]
    filer = opts[:filer]
    compress? = opts[:compress?]
    bf = BloomFilter.new(opts)
    dt = %__MODULE__{bloom_filter: bf, level_key: opts[:level_key]}

    stream
    |> Stream.transform(
      fn -> {nil, 0, nil, []} end,
      fn
        _, {:halt, file} ->
          FileIO.close(file)
          {:halt, nil}

        triple, {file, boundary, acc, offset_index} ->
          file = file || FileIO.open!(filer.(), write?: true)
          acc = acc || %{dt | id: file.path}
          {key, _, _} = triple

          with {:ok, acc, offset_index, boundary} <-
                 maybe_append_index(file, acc, offset_index, key, boundary, compress?),
               {:ok, acc, offset_index} <-
                 append_data(file, acc, offset_index, triple, compress?),
               {:ok, acc, wrapped?} <-
                 maybe_append_footer(file, acc, offset_index, max_size, compress?) do
            if wrapped?,
              do: {[{:ok, acc}], {nil, 0, nil, []}},
              else: {[], {file, boundary, acc, offset_index}}
          else
            error -> {[error], {:halt, file}}
          end
      end,
      fn
        {%FileIO{} = file, _, acc, offset_index} ->
          case maybe_append_footer(file, acc, offset_index, 0, compress?) do
            {:ok, acc, _} -> {[{:ok, acc}], nil}
            error -> {[error], {:halt, file}}
          end

        _ ->
          {[], nil}
      end,
      fn
        {%FileIO{} = file, _, _, _} -> FileIO.close(file)
        {:halt, %FileIO{} = file} -> FileIO.close(file)
        _ -> :ok
      end
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, dt}, {:ok, dts} -> {:cont, {:ok, [dt | dts]}}
      error, _acc -> {:halt, error}
    end)
  end

  @spec from_file(Path.t()) :: {:ok, t()} | {:error, term()}
  def from_file(path) do
    io = FileIO.open!(path)

    try do
      case FileIO.read_footer(io) do
        {:ok, %__MODULE__{} = dt} -> {:ok, dt}
        {:ok, _} -> {:error, :invalid_disk_table}
        error -> error
      end
    after
      FileIO.close(io)
    end
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(dt, key) do
    within_min_max?(dt, key) and bloom_filter_member?(dt, key)
  end

  @spec search(t(), list(term()), non_neg_integer()) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def search(dt, keys, seq) do
    Stream.transform(
      keys,
      fn -> FileIO.open!(dt.id) end,
      fn key, io ->
        case lookup(io, dt.index, key, seq) do
          {:ok, triple} -> {[triple], io}
          :not_found -> {[], io}
          :eof -> {:halt, io}
          {:error, reason} -> raise IOError, operation: :search, path: dt.id, reason: reason
        end
      end,
      fn io -> FileIO.close(io) end
    )
  end

  @spec stream(t(), keyword()) :: Enumerable.t({term(), non_neg_integer(), term()})
  def stream(dt, opts \\ []) do
    {min, max} = opts[:bounds] || dt.key_range
    min = min || elem(dt.key_range, 0)
    max = max || elem(dt.key_range, 1)
    seq = opts[:seq] || :infinity

    if within_bounds?(dt, min, max) do
      Stream.resource(
        fn ->
          with {:ok, io} <- FileIO.open(dt.id),
               block_offset = block_offset_lookup(dt.index, min),
               {:ok, {:index, offset_index}} <-
                 FileIO.read(io, verify_crc?: false, offset: block_offset),
               min_offset = min_key_offset_lookup(offset_index, min),
               pos = if(min_offset == :spill, do: block_offset, else: min_offset),
               :ok <- FileIO.set_position(io, pos) do
            io
          else
            {:error, reason} -> raise IOError, operation: :stream, path: dt.id, reason: reason
          end
        end,
        fn io ->
          case FileIO.read(io, verify_crc?: false) do
            {:ok, {k, _, _}} when k > max ->
              {:halt, io}

            {:ok, {_, s, _} = triple} when s < seq ->
              {[triple], io}

            {:ok, %__MODULE__{}} ->
              {:halt, io}

            {:ok, _} ->
              {[], io}

            :eof ->
              {:halt, io}

            {:error, reason} ->
              FileIO.close(io)
              raise IOError, operation: :stream, path: dt.id, reason: reason
          end
        end,
        fn io -> FileIO.close(io) end
      )
    else
      []
    end
  end

  defp maybe_append_index(
         file,
         %{size: size} = dt,
         [{last, _, _} | _] = offset_index,
         key,
         boundary,
         compress?
       )
       when size - boundary >= @index_interval and last != key do
    with {:ok, dt, offset_index} <- append_index(file, dt, offset_index, compress?) do
      {:ok, dt, offset_index, dt.size}
    end
  end

  defp maybe_append_index(_, dt, offset_index, _, boundary, _),
    do: {:ok, dt, offset_index, boundary}

  defp maybe_append_footer(_, %{size: size} = dt, _, max_size, _) when size < max_size,
    do: {:ok, dt, false}

  defp maybe_append_footer(file, dt, offset_index, _, compress?) do
    with {:ok, dt, _} <- append_index(file, dt, offset_index, compress?),
         dt = finalize_table(dt),
         :ok <- append_footer(file, dt, compress?) do
      {:ok, dt, true}
    end
  end

  defp append_data(file, dt, offset_index, triple, compress?) do
    {key, seq, _} = triple

    with {:ok, size} <- FileIO.append(file, triple, compress?: compress?) do
      offset_index = [{key, seq, dt.size} | offset_index]
      {:ok, update_table(dt, triple, size), offset_index}
    end
  end

  defp append_index(_, dt, [], _), do: {:ok, dt, []}

  defp append_index(file, dt, offset_index, compress?) do
    offset_index = offset_index |> Enum.reverse() |> List.to_tuple()
    {first_key, _, _} = elem(offset_index, 0)

    with {:ok, inc_size} <- FileIO.append(file, {:index, offset_index}, compress?: compress?) do
      index = [{first_key, dt.size} | dt.index]
      size = dt.size + inc_size
      dt = %{dt | size: size, index: index}
      {:ok, dt, []}
    end
  end

  defp append_footer(file, dt, compress?) do
    with {:ok, _} <- FileIO.append(file, dt, compress?: compress?, footer?: true),
         :ok <- FileIO.sync(file) do
      FileIO.close(file)
    end
  end

  defp update_table(dt, triple, size) do
    {key, seq, _val} = triple

    key_range =
      case dt.key_range do
        nil -> {key, key}
        {min, _} -> {min, key}
      end

    seq_range =
      case dt.seq_range do
        nil -> {seq, seq}
        {min, _} -> {min, seq}
      end

    bloom_filter = BloomFilter.put(dt.bloom_filter, key)

    %{
      dt
      | key_range: key_range,
        seq_range: seq_range,
        bloom_filter: bloom_filter,
        size: dt.size + size
    }
  end

  defp finalize_table(dt),
    do: %{dt | index: Enum.reverse(dt.index) |> List.to_tuple()}

  defp lookup(io, index, key, seq) do
    block_offset = block_offset_lookup(index, key)

    with {:ok, {:index, offset_index}} <-
           FileIO.read(io, verify_crc?: false, offset: block_offset) do
      case key_offset_lookup(offset_index, key, seq) do
        :not_found -> :not_found
        key_offset -> key_lookup(io, key, key_offset)
      end
    end
  end

  defp key_lookup(io, key, offset) do
    case FileIO.read(io, offset: offset, verify_crc?: false) do
      {:ok, {k, _, _} = triple} when k == key -> {:ok, triple}
      {:ok, _} -> :not_found
      error -> error
    end
  end

  defp block_offset_lookup(index, target_key) do
    point = partition_point(index, fn {key, _} -> key <= target_key end)
    {_, offset} = elem(index, max(point - 1, 0))
    offset
  end

  defp key_offset_lookup(index, target_key, target_seq) do
    point =
      partition_point(index, fn {key, seq, _} ->
        {key, -seq} <= {target_key, -target_seq}
      end)

    if point < tuple_size(index) do
      case elem(index, point) do
        {k, s, offset} when k == target_key and s < target_seq -> offset
        _ -> :not_found
      end
    else
      :not_found
    end
  end

  defp min_key_offset_lookup(index, min) do
    point = partition_point(index, fn {key, _, _} -> key < min end)

    if point < tuple_size(index) do
      {_, _, offset} = elem(index, point)
      offset
    else
      :spill
    end
  end

  defp partition_point(index, pred),
    do: partition_point(index, pred, 0, tuple_size(index))

  defp partition_point(_, _, lo, lo), do: lo

  defp partition_point(index, pred, lo, hi) do
    mid = div(lo + hi, 2)

    if pred.(elem(index, mid)),
      do: partition_point(index, pred, mid + 1, hi),
      else: partition_point(index, pred, lo, mid)
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
