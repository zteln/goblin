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
    dt = %__MODULE__{level_key: opts[:level_key], index: MemIndex.new()}

    stream
    |> Stream.transform(
      fn ->
        %{
          file: nil,
          boundary: 0,
          keys: {0, []},
          disk_table: nil,
          index: DiskIndex.new(),
          compress?: opts[:compress?],
          filer: opts[:filer],
          max_size: opts[:max_size],
          fpp: opts[:fpp]
        }
      end,
      fn
        _, {:halt, acc} ->
          {:halt, acc}

        {key, _, _} = triple, acc ->
          with {:ok, acc} <- maybe_init(acc, dt),
               {:ok, acc} <- maybe_append_index(acc, key),
               {:ok, acc} <- append_data(acc, triple),
               {:ok, acc, out} <- maybe_finalize(acc) do
            {out, acc}
          else
            error -> {[error], {:halt, acc}}
          end
      end,
      fn
        %{file: %FileIO{}} = acc ->
          case finalize(acc) do
            {:ok, _acc, out} -> {out, nil}
            error -> {[error], {:halt, acc}}
          end

        _ ->
          {[], nil}
      end,
      fn
        %{file: %FileIO{} = file} -> FileIO.close(file)
        {:halt, %{file: %FileIO{} = file}} -> FileIO.close(file)
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
          {:error, :not_found} -> {[], io}
          {:error, :eof} -> {:halt, io}
          {:error, reason} -> raise IOError, operation: :search, path: dt.id, reason: reason
        end
      end,
      fn io -> FileIO.close(io) end
    )
  end

  @spec stream(t()) :: Enumerable.t({term(), non_neg_integer(), term()})
  @spec stream(t(), term(), term(), non_neg_integer()) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def stream(dt) do
    {min, max} = dt.key_range
    stream_table(dt, min, max, :infinity)
  end

  def stream(dt, min, max, seq) do
    {dt_min, dt_max} = dt.key_range
    min = if min == :"$goblin_nil", do: dt_min, else: min
    max = if max == :"$goblin_nil", do: dt_max, else: max

    if within_bounds?(dt, min, max),
      do: stream_table(dt, min, max, seq),
      else: []
  end

  defp stream_table(dt, min, max, seq) do
    Stream.resource(
      fn ->
        disk_index_offset = MemIndex.lookup_offset(dt.index, min)

        with {:ok, io} <- FileIO.open(dt.id),
             :ok <- set_position_to_min(io, min, disk_index_offset) do
          io
        else
          {:error, reason} -> raise IOError, operation: :stream, path: dt.id, reason: reason
        end
      end,
      fn io ->
        case FileIO.seq_read(io, verify_crc?: false) do
          {:ok, {k, _, _}} when k > max -> {:halt, io}
          {:ok, {_, s, _} = triple} when s < seq -> {[triple], io}
          {:ok, %__MODULE__{}} -> {:halt, io}
          {:ok, _} -> {[], io}
          {:error, :eof} -> {:halt, io}
          {:error, reason} -> raise IOError, operation: :stream, path: dt.id, reason: reason
        end
      end,
      fn io -> FileIO.close(io) end
    )
  end

  defp set_position_to_min(io, min, offset) do
    with {:ok, {:index, disk_index}} <- FileIO.offset_read(io, offset, verify_crc?: false) do
      min_offset =
        case DiskIndex.lookup(disk_index, fn {key, _, _} -> key < min end) do
          {_, _, offset} -> offset
          nil -> offset
        end

      FileIO.set_position(io, min_offset)
    end
  end

  defp maybe_init(%{file: nil, disk_table: nil} = acc, new_dt) do
    with {:ok, file} <- FileIO.open(acc.filer.(), write?: true) do
      {:ok,
       %{
         acc
         | file: file,
           disk_table: %{new_dt | id: file.path},
           boundary: 0,
           index: DiskIndex.new(),
           keys: {0, []}
       }}
    end
  end

  defp maybe_init(acc, _new_dt), do: {:ok, acc}

  defp maybe_append_index(
         %{disk_table: %{size: size}, boundary: boundary, index: [{last, _, _} | _]} = acc,
         key
       )
       when size - boundary >= @index_interval and last != key do
    append_index(acc)
  end

  defp maybe_append_index(acc, _key), do: {:ok, acc}

  defp maybe_finalize(%{disk_table: %{size: size}, max_size: max_size} = acc)
       when size >= max_size, do: finalize(acc)

  defp maybe_finalize(acc), do: {:ok, acc, []}

  defp finalize(acc) do
    acc = finalize_bloom_filter(acc)

    with {:ok, %{disk_table: dt} = acc} <- append_and_finalize_index(acc),
         {:ok, acc} <- append_footer(acc) do
      {:ok, acc, [{:ok, dt}]}
    end
  end

  defp append_footer(acc) do
    with {:ok, _} <-
           FileIO.append(acc.file, acc.disk_table, compress?: acc.compress?, footer?: true),
         :ok <- FileIO.sync(acc.file),
         :ok <- FileIO.close(acc.file) do
      {:ok, %{acc | file: nil, disk_table: nil}}
    end
  end

  defp append_and_finalize_index(acc) do
    with {:ok, acc} <- append_index(acc) do
      dt = %{acc.disk_table | index: MemIndex.finalize(acc.disk_table.index)}
      {:ok, %{acc | disk_table: dt}}
    end
  end

  defp append_index(acc) do
    dt = acc.disk_table
    {start, disk_index} = DiskIndex.finalize(acc.index)

    with {:ok, inc_size} <-
           FileIO.append(acc.file, {:index, disk_index}, compress?: acc.compress?) do
      dt = %{dt | size: dt.size + inc_size, index: MemIndex.append(dt.index, start, dt.size)}
      {:ok, %{acc | disk_table: dt, index: DiskIndex.new(), boundary: dt.size}}
    end
  end

  defp append_data(acc, triple) do
    {key, seq, _} = triple

    keys =
      case acc.keys do
        {no_keys, [^key | _] = keys} -> {no_keys, keys}
        {no_keys, keys} -> {no_keys + 1, [key | keys]}
      end

    with {:ok, size} <- FileIO.append(acc.file, triple, compress?: acc.compress?) do
      disk_index = DiskIndex.append(acc.index, key, seq, acc.disk_table.size)
      dt = update_table(acc.disk_table, triple, size)
      {:ok, %{acc | disk_table: dt, index: disk_index, keys: keys}}
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

    %{
      dt
      | key_range: key_range,
        seq_range: seq_range,
        size: dt.size + size
    }
  end

  defp finalize_bloom_filter(acc) do
    {no_keys, keys} = acc.keys
    bf = BloomFilter.new(no_keys, keys, acc.fpp)
    dt = %{acc.disk_table | bloom_filter: bf}
    %{acc | disk_table: dt}
  end

  defp lookup(io, index, key, seq) do
    disk_index_pos = MemIndex.lookup_offset(index, key)

    with {:ok, {:index, disk_index}} <-
           FileIO.offset_read(io, disk_index_pos, verify_crc?: false),
         {:ok, key_offset} <- key_offset_lookup(disk_index, key, seq) do
      key_lookup(io, key, key_offset)
    end
  end

  defp key_lookup(io, key, offset) do
    case FileIO.offset_read(io, offset, verify_crc?: false) do
      {:ok, {k, _, _} = triple} when k == key -> {:ok, triple}
      {:ok, _} -> {:error, :not_found}
      error -> error
    end
  end

  defp key_offset_lookup(disk_index, target_key, target_seq) do
    case DiskIndex.lookup(disk_index, fn {key, seq, _} ->
           {key, -seq} <= {target_key, -target_seq}
         end) do
      {k, s, offset} when k == target_key and s < target_seq -> {:ok, offset}
      _ -> {:error, :not_found}
    end
  end

  defp within_min_max?(%{key_range: {min, max}}, key),
    do: min <= key and key <= max

  defp bloom_filter_member?(dt, key),
    do: BloomFilter.member?(dt.bloom_filter, key)

  defp within_bounds?(%{key_range: {min1, max1}}, min2, max2),
    do: min1 <= max2 and min2 <= max1
end
