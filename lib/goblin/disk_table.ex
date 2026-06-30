defmodule Goblin.DiskTable do
  @moduledoc false

  alias Goblin.{
    BloomFilter,
    FileIO,
    IOError
  }

  alias Goblin.DiskTable.{MemIndex, DiskIndex}

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
          index: MemIndex.t(),
          size: non_neg_integer()
        }

  @spec build(Enumerable.t({term(), non_neg_integer(), term()}), keyword()) ::
          {:ok, list(t())} | {:error, term()}
  def build(stream, opts) do
    max_size = opts[:max_size]
    filer = opts[:filer]
    compress? = opts[:compress?]
    bf = BloomFilter.new(opts)
    dt = %__MODULE__{bloom_filter: bf, level_key: opts[:level_key], index: MemIndex.new()}

    stream
    |> Stream.transform(
      fn -> {nil, 0, nil, DiskIndex.new()} end,
      fn
        _, {:halt, file} ->
          FileIO.close(file)
          {:halt, nil}

        triple, {file, boundary, acc, disk_index} ->
          file = file || FileIO.open!(filer.(), write?: true)
          acc = acc || %{dt | id: file.path}
          {key, _, _} = triple

          with {:ok, acc, disk_index, boundary} <-
                 maybe_append_index(file, acc, disk_index, key, boundary, compress?),
               {:ok, acc, disk_index} <-
                 append_data(file, acc, disk_index, triple, compress?),
               {:ok, acc, wrapped?} <-
                 maybe_append_footer(file, acc, disk_index, max_size, compress?) do
            if wrapped?,
              do: {[{:ok, acc}], {nil, 0, nil, DiskIndex.new()}},
              else: {[], {file, boundary, acc, disk_index}}
          else
            error -> {[error], {:halt, file}}
          end
      end,
      fn
        {%FileIO{} = file, _, acc, disk_index} ->
          case maybe_append_footer(file, acc, disk_index, 0, compress?) do
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
               disk_index_offset = MemIndex.lookup_offset(dt.index, min),
               {:ok, {:index, disk_index}} <-
                 FileIO.offset_read(io, disk_index_offset, verify_crc?: false),
               min_offset = min_key_offset_lookup(disk_index, min),
               :ok <- FileIO.set_position(io, min_offset || disk_index_offset) do
            io
          else
            {:error, reason} -> raise IOError, operation: :stream, path: dt.id, reason: reason
          end
        end,
        fn io ->
          case FileIO.seq_read(io, verify_crc?: false) do
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
         [{last, _, _} | _] = disk_index,
         key,
         boundary,
         compress?
       )
       when size - boundary >= @index_interval and last != key do
    with {:ok, dt, disk_index} <- append_index(file, dt, disk_index, compress?) do
      {:ok, dt, disk_index, dt.size}
    end
  end

  defp maybe_append_index(_, dt, disk_index, _, boundary, _),
    do: {:ok, dt, disk_index, boundary}

  defp maybe_append_footer(_, %{size: size} = dt, _, max_size, _) when size < max_size,
    do: {:ok, dt, false}

  defp maybe_append_footer(file, dt, disk_index, _, compress?) do
    with {:ok, dt, _} <- append_index(file, dt, disk_index, compress?),
         dt = %{dt | index: MemIndex.finalize(dt.index)},
         :ok <- append_footer(file, dt, compress?) do
      {:ok, dt, true}
    end
  end

  defp append_data(file, dt, disk_index, triple, compress?) do
    {key, seq, _} = triple

    with {:ok, size} <- FileIO.append(file, triple, compress?: compress?) do
      disk_index = DiskIndex.append(disk_index, key, seq, dt.size)
      {:ok, update_table(dt, triple, size), disk_index}
    end
  end

  defp append_index(_, dt, [], _), do: {:ok, dt, []}

  defp append_index(file, dt, disk_index, compress?) do
    {start, disk_index} = DiskIndex.finalize(disk_index)

    with {:ok, inc_size} <- FileIO.append(file, {:index, disk_index}, compress?: compress?) do
      dt = %{dt | size: dt.size + inc_size, index: MemIndex.append(dt.index, start, dt.size)}
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

  defp lookup(io, index, key, seq) do
    disk_index_pos = MemIndex.lookup_offset(index, key)

    with {:ok, {:index, disk_index}} <-
           FileIO.offset_read(io, disk_index_pos, verify_crc?: false) do
      case key_offset_lookup(disk_index, key, seq) do
        :not_found -> :not_found
        key_offset -> key_lookup(io, key, key_offset)
      end
    end
  end

  defp key_lookup(io, key, offset) do
    case FileIO.offset_read(io, offset, verify_crc?: false) do
      {:ok, {k, _, _} = triple} when k == key -> {:ok, triple}
      {:ok, _} -> :not_found
      error -> error
    end
  end

  defp key_offset_lookup(disk_index, target_key, target_seq) do
    case DiskIndex.lookup(disk_index, fn {key, seq, _} ->
           {key, -seq} <= {target_key, -target_seq}
         end) do
      {k, s, offset} when k == target_key and s < target_seq -> offset
      _ -> :not_found
    end
  end

  defp min_key_offset_lookup(disk_index, min) do
    with {_, _, offset} <- DiskIndex.lookup(disk_index, fn {key, _, _} -> key < min end) do
      offset
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
