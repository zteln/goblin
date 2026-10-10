defmodule Goblin.DiskTable do
  @moduledoc false

  alias Goblin.BloomFilter
  alias Goblin.Persistence
  alias Goblin.IOError
  alias Goblin.DiskTable.{MemIndex, DiskIndex}

  @block_interval 4096
  @version 0

  defstruct [
    :id,
    :level_key,
    :bloom_filter,
    :key_range,
    :sqn_range,
    index: [],
    size: 0
  ]

  @type t :: %__MODULE__{
          id: Path.t(),
          level_key: non_neg_integer(),
          bloom_filter: BloomFilter.t(),
          key_range: {term(), term()},
          sqn_range: {non_neg_integer(), non_neg_integer()},
          index: MemIndex.t(),
          size: non_neg_integer()
        }

  @spec delete(t()) :: :ok | {:error, term()}
  def delete(dt), do: Persistence.remove(dt.id)

  @spec build(Enumerable.t({term(), non_neg_integer(), term()}), keyword()) ::
          {:ok, list(t())} | {:error, term()}
  def build(stream, opts) do
    acc = %{table: nil, tables: []}

    Enum.reduce_while(stream, {:ok, acc}, fn triple, {:ok, acc} ->
      {key, _, _} = triple

      with {:ok, acc} <- maybe_finalize(acc, key, opts),
           {:ok, acc} <- maybe_open(acc, opts),
           {:ok, acc} <- maybe_append_block(acc, key, opts),
           {:ok, acc} <- append_data(acc, triple, opts) do
        {:cont, {:ok, acc}}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> with {:ok, acc} <- finalize(acc, opts), do: {:ok, acc.tables}
      error -> error
    end
  end

  @spec from_file(Path.t()) :: {:ok, t()} | {:error, term()}
  def from_file(path) do
    with {:ok, io} <- Persistence.open(path) do
      try do
        case Persistence.read_footer(io) do
          {:ok, {:footer, {@version, dt}}} -> {:ok, struct(__MODULE__, %{dt | id: path})}
          {:ok, _} -> {:error, :invalid_disk_table}
          error -> error
        end
      after
        Persistence.close(io)
      end
    end
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(dt, key) do
    within_min_max?(dt, key) and bloom_filter_member?(dt, key)
  end

  @spec search(t(), list(term()), non_neg_integer()) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def search(dt, keys, sqn) do
    Stream.transform(
      keys,
      fn -> Persistence.open!(dt.id) end,
      fn key, io ->
        case lookup(io, dt.index, key, sqn) do
          {:ok, triple} -> {[triple], io}
          {:error, :not_found} -> {[], io}
          {:error, reason} -> raise IOError, operation: :search, path: dt.id, reason: reason
        end
      end,
      fn io -> Persistence.close(io) end
    )
  end

  @spec stream(t()) :: Enumerable.t({term(), non_neg_integer(), term()})
  @spec stream(t(), term(), term(), non_neg_integer()) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def stream(dt) do
    {min, max} = dt.key_range
    stream_table(dt, min, max, :infinity)
  end

  def stream(dt, min, max, sqn) do
    if within_bounds?(dt, min, max),
      do: stream_table(dt, min, max, sqn),
      else: []
  end

  defp stream_table(dt, min, max, sqn) do
    Stream.resource(
      fn ->
        disk_index_offset = MemIndex.lookup_offset(dt.index, min)

        with {:ok, io} <- Persistence.open(dt.id),
             :ok <- set_position_to_min(io, min, disk_index_offset) do
          io
        else
          {:error, reason} -> raise IOError, operation: :stream, path: dt.id, reason: reason
        end
      end,
      fn io ->
        case Persistence.seq_read(io) do
          {:ok, {:footer, _}} -> {:halt, io}
          {:ok, {k, _, _}} when k > max -> {:halt, io}
          {:ok, {_, s, _} = triple} when s < sqn -> {[triple], io}
          {:ok, _} -> {[], io}
          {:error, :eof} -> {:halt, io}
          {:error, reason} -> raise IOError, operation: :stream, path: dt.id, reason: reason
        end
      end,
      fn io -> Persistence.close(io) end
    )
  end

  defp set_position_to_min(io, min, offset) do
    with {:ok, {:index, disk_index}} <- Persistence.offset_read(io, offset) do
      min_offset =
        case DiskIndex.lookup(disk_index, fn {key, _, _} -> key < min end) do
          {_, _, offset} -> offset
          nil -> offset
        end

      Persistence.set_position(io, min_offset)
    else
      {:ok, _} -> {:error, :invalid_index}
      error -> error
    end
  end

  defp maybe_open(%{table: nil} = acc, opts) do
    with {:ok, file} <- Persistence.open(opts[:filer].(), write?: true, new?: true) do
      dt = %__MODULE__{id: file.path, level_key: opts[:level_key], index: MemIndex.new()}
      tab = %{file: file, disk_table: dt, boundary: 0, block: DiskIndex.new(), keys: {0, []}}
      {:ok, %{acc | table: tab}}
    end
  end

  defp maybe_open(acc, _opts), do: {:ok, acc}

  defp maybe_append_block(
         %{
           table: %{
             disk_table: %{size: size},
             boundary: boundary,
             block: [{last, _, _} | _]
           }
         } = acc,
         key,
         opts
       )
       when size - boundary >= @block_interval and last != key do
    append_block(acc, opts)
  end

  defp maybe_append_block(acc, _key, _opts), do: {:ok, acc}

  defp maybe_finalize(%{table: nil} = acc, _key, _opts), do: {:ok, acc}

  defp maybe_finalize(acc, key, opts) do
    %{disk_table: %{size: size}, keys: {_, [last | _]}} = acc.table
    if size >= opts[:max_size] and last != key, do: finalize(acc, opts), else: {:ok, acc}
  end

  defp finalize(%{table: nil} = acc, _opts), do: {:ok, acc}

  defp finalize(acc, opts) do
    acc = finalize_bloom_filter(acc, opts)

    with {:ok, %{table: %{disk_table: dt}} = acc} <- append_and_finalize_index(acc, opts),
         {:ok, acc} <- append_footer(acc, opts) do
      {:ok, %{acc | tables: [dt | acc.tables]}}
    end
  end

  defp append_footer(acc, opts) do
    %{disk_table: dt, file: file} = acc.table
    footer = Map.from_struct(dt)

    with {:ok, _} <-
           Persistence.append(
             file,
             {:footer, {@version, footer}},
             compress?: opts[:compress?],
             footer?: true
           ),
         :ok <- Persistence.sync(file),
         :ok <- Persistence.close(file) do
      {:ok, %{acc | table: nil}}
    end
  end

  defp append_and_finalize_index(acc, opts) do
    with {:ok, acc} <- append_block(acc, opts) do
      %{disk_table: dt} = acc.table
      dt = %{dt | index: MemIndex.finalize(dt.index)}
      tab = %{acc.table | disk_table: dt}
      {:ok, %{acc | table: tab}}
    end
  end

  defp append_block(acc, opts) do
    %{disk_table: dt, block: block, file: file} = acc.table
    {start, block} = DiskIndex.finalize(block)

    with {:ok, inc_size} <-
           Persistence.append(file, {:index, block}, compress?: opts[:compress?]) do
      dt = %{dt | size: dt.size + inc_size, index: MemIndex.append(dt.index, start, dt.size)}
      tab = %{acc.table | disk_table: dt, block: DiskIndex.new(), boundary: dt.size}
      {:ok, %{acc | table: tab}}
    end
  end

  defp append_data(acc, triple, opts) do
    {key, sqn, _} = triple
    %{disk_table: dt, file: file, block: block, keys: keys} = acc.table

    keys =
      case keys do
        {no_keys, [^key | _] = keys} -> {no_keys, keys}
        {no_keys, keys} -> {no_keys + 1, [key | keys]}
      end

    with {:ok, size} <- Persistence.append(file, triple, compress?: opts[:compress?]) do
      block = DiskIndex.append(block, key, sqn, dt.size)
      dt = update_table(dt, triple, size)
      tab = %{acc.table | disk_table: dt, block: block, keys: keys}
      {:ok, %{acc | table: tab}}
    end
  end

  defp update_table(dt, triple, size) do
    {key, sqn, _val} = triple

    key_range =
      case dt.key_range do
        nil -> {key, key}
        {min, _} -> {min, key}
      end

    sqn_range =
      case dt.sqn_range do
        nil -> {sqn, sqn}
        {min, max} -> {min(min, sqn), max(max, sqn)}
      end

    %{
      dt
      | key_range: key_range,
        sqn_range: sqn_range,
        size: dt.size + size
    }
  end

  defp finalize_bloom_filter(acc, opts) do
    %{disk_table: dt, keys: {no_keys, keys}} = acc.table
    bf = BloomFilter.new(no_keys, keys, opts[:fpp])
    dt = %{dt | bloom_filter: bf}
    tab = %{acc.table | disk_table: dt}
    %{acc | table: tab}
  end

  defp lookup(io, index, key, sqn) do
    disk_index_pos = MemIndex.lookup_offset(index, key)

    with {:ok, {:index, disk_index}} <- Persistence.offset_read(io, disk_index_pos),
         {:ok, key_offset} <- key_offset_lookup(disk_index, key, sqn) do
      key_lookup(io, key, key_offset)
    else
      {:ok, _} -> {:error, :invalid_index}
      error -> error
    end
  end

  defp key_lookup(io, key, offset) do
    case Persistence.offset_read(io, offset) do
      {:ok, {k, _, _} = triple} when k == key -> {:ok, triple}
      {:ok, _} -> {:error, :not_found}
      error -> error
    end
  end

  defp key_offset_lookup(disk_index, target_key, target_sqn) do
    case DiskIndex.lookup(disk_index, fn {key, sqn, _} ->
           {key, -sqn} <= {target_key, -target_sqn}
         end) do
      {k, s, offset} when k == target_key and s < target_sqn -> {:ok, offset}
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
