defmodule Goblin.DiskTables.DiskTable do
  @moduledoc false
  alias Goblin.BloomFilter
  alias Goblin.DiskTables.{Encoder, Handler}

  defstruct [
    :file,
    :level_key,
    :bloom_filter,
    key_range: {nil, nil},
    seq_range: {nil, 0},
    size: 0,
    no_blocks: 0,
    crc: :erlang.crc32(<<>>)
  ]

  @type t :: %__MODULE__{}

  @doc "Writes new disk tables from the provided stream."
  @spec write_new(
          Enumerable.t(Goblin.triple()),
          keyword()
        ) :: {:ok, [t()]} | {:error, term()}
  def write_new(data_stream, opts) do
    max_sst_size = opts[:max_sst_size]
    compress? = opts[:compress?]

    data_stream
    |> Stream.transform(
      fn ->
        nil
      end,
      fn
        _triple, :halt ->
          {:halt, nil}

        triple, acc ->
          {disk_table, handler, tmp_file} = acc || open_new_disk_table(opts)

          case append_to_disk_table(triple, disk_table, handler, compress?) do
            {:ok, %{size: size} = disk_table, handler} when size >= max_sst_size ->
              case finalize_disk_table(disk_table, handler, tmp_file, compress?) do
                {:ok, disk_table} -> {[{:ok, disk_table}], nil}
                error -> {[error], :halt}
              end

            {:ok, disk_table, handler} ->
              {[], {disk_table, handler, tmp_file}}

            error ->
              {[error], :halt}
          end
      end,
      fn
        nil ->
          {[], nil}

        {disk_table, handler, tmp_file} ->
          case finalize_disk_table(disk_table, handler, tmp_file, compress?) do
            {:ok, disk_table} -> {[{:ok, disk_table}], nil}
            error -> {[error], nil}
          end
      end,
      fn
        nil -> :ok
        :halt -> :ok
        {_disk_table, handler, _tmp_file} -> Handler.close(handler)
      end
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, disk_table}, {:ok, acc} -> {:cont, {:ok, [disk_table | acc]}}
      error, {:ok, _acc} -> {:halt, error}
    end)
  end

  defp open_new_disk_table(opts) do
    {tmp_file, file} = opts[:next_file_f].()
    handler = Handler.open!(tmp_file, write?: true)

    disk_table = %__MODULE__{
      file: file,
      level_key: opts[:level_key],
      bloom_filter: BloomFilter.new(bit_array_size: opts[:bf_bit_array_size], fpp: opts[:bf_fpp])
    }

    {disk_table, handler, tmp_file}
  end

  defp append_to_disk_table({key, seq, _val} = triple, disk_table, handler, compress?) do
    {sst_block, no_blocks} = Encoder.encode_sst_block(triple, compress?)

    key_range =
      case disk_table.key_range do
        {nil, _max} -> {key, key}
        {min, _max} -> {min, key}
      end

    seq_range =
      case disk_table.seq_range do
        {nil, _max} -> {seq, seq}
        {min, _max} -> {min, seq}
      end

    disk_table = %{
      disk_table
      | key_range: key_range,
        seq_range: seq_range,
        bloom_filter: BloomFilter.put(disk_table.bloom_filter, key),
        crc: Encoder.update_crc(disk_table.crc, sst_block),
        no_blocks: disk_table.no_blocks + no_blocks,
        size: disk_table.size + no_blocks * Encoder.sst_block_unit_size()
    }

    case Handler.write(handler, sst_block) do
      {:ok, handler} ->
        {:ok, disk_table, handler}

      error ->
        Handler.close(handler)
        error
    end
  end

  defp finalize_disk_table(disk_table, handler, tmp_file, compress?) do
    {footer_block, size, crc} =
      Encoder.encode_footer_block(
        disk_table.level_key,
        disk_table.bloom_filter,
        disk_table.key_range,
        disk_table.seq_range,
        handler.file_offset,
        disk_table.no_blocks,
        disk_table.crc,
        disk_table.size,
        compress?
      )

    with {:ok, handler} <- Handler.write(handler, footer_block),
         :ok <- Handler.sync(handler),
         :ok <- Handler.close(handler),
         :ok <- File.rename(tmp_file, disk_table.file) do
      {:ok, %{disk_table | size: size, crc: crc}}
    else
      error ->
        Handler.close(handler)
        error
    end
  end

  @doc "Read and parse a path to a disk table, returning a disk table struct."
  @spec parse(Path.t()) :: {:ok, t()} | {:error, term()}
  def parse(file) do
    handler = Handler.open!(file)

    try do
      with :ok <- verify_magic(handler),
           {:ok, size} <- parse_size(handler),
           {:ok, crc} <- parse_crc(handler),
           :ok <- verify_crc(handler, size, crc),
           {:ok,
            {
              level_key,
              bf_block_info,
              key_range_block_info,
              seq_range_block_info,
              no_blocks,
              _compressed?
            }} <- parse_metadata(handler),
           {:ok, bloom_filter} <- parse_bloom_filter(handler, bf_block_info),
           {:ok, key_range} <- parse_key_range(handler, key_range_block_info),
           {:ok, seq_range} <- parse_seq_range(handler, seq_range_block_info) do
        {:ok,
         %__MODULE__{
           file: file,
           level_key: level_key,
           no_blocks: no_blocks,
           size: size,
           crc: crc,
           bloom_filter: bloom_filter,
           key_range: key_range,
           seq_range: seq_range
         }}
      end
    after
      Handler.close(handler)
    end
  end

  @doc "Checks whether a key is within the minmax range of a disk table."
  @spec within_min_max?(t(), Goblin.db_key()) :: boolean()
  def within_min_max?(disk_table, key) do
    %{key_range: {min, max}} = disk_table
    min <= key and key <= max
  end

  @doc "Checks whether a key is a member to a disk tables Bloom filter."
  @spec bloom_filter_member?(t(), Goblin.db_key()) :: boolean()
  def bloom_filter_member?(disk_table, key) do
    BloomFilter.member?(disk_table.bloom_filter, key)
  end

  @doc "Checks whether a disk table overlaps the provided range."
  @spec within_bounds?(t(), Goblin.db_key() | nil, Goblin.db_key() | nil) :: boolean()
  def within_bounds?(_disk_table, nil, nil), do: true

  def within_bounds?(disk_table, min, nil) do
    %{key_range: {_, max}} = disk_table
    min <= max
  end

  def within_bounds?(disk_table, nil, max) do
    %{key_range: {min, _}} = disk_table
    min <= max
  end

  def within_bounds?(disk_table, min, max) do
    %{key_range: {disk_table_min, disk_table_max}} = disk_table
    min <= disk_table_max and disk_table_min <= max
  end

  defp verify_magic(handler) do
    with {:ok, magic_block} <-
           Handler.read_from_end(handler, Encoder.magic_size(), Encoder.magic_size()) do
      Encoder.validate_magic_block(magic_block)
    end
  end

  defp verify_crc(handler, start \\ 0, finish, target_crc, crc \\ :erlang.crc32(<<>>))

  defp verify_crc(_handler, finish, finish, target_crc, target_crc), do: :ok
  defp verify_crc(_handler, finish, finish, _target_crc, _crc), do: {:error, :invalid_crc}

  defp verify_crc(handler, start, finish, target_crc, crc) do
    inc = min(finish - start, Encoder.sst_block_unit_size())

    with {:ok, chunk} <- Handler.read(handler, start, inc) do
      crc = :erlang.crc32(crc, chunk)
      verify_crc(handler, start + inc, finish, target_crc, crc)
    end
  end

  defp parse_size(handler) do
    with {:ok, size_block} <-
           Handler.read_from_end(
             handler,
             Encoder.magic_size() + Encoder.size_block_size(),
             Encoder.size_block_size()
           ) do
      Encoder.decode_size_block(size_block)
    end
  end

  defp parse_crc(handler) do
    with {:ok, crc_block} <-
           Handler.read_from_end(
             handler,
             Encoder.magic_size() + Encoder.size_block_size() + Encoder.crc_block_size(),
             Encoder.crc_block_size()
           ) do
      Encoder.decode_crc_block(crc_block)
    end
  end

  defp parse_metadata(handler) do
    with {:ok, metadata_block} <-
           Handler.read_from_end(
             handler,
             Encoder.magic_size() + Encoder.size_block_size() + Encoder.crc_block_size() +
               Encoder.metadata_block_size(),
             Encoder.metadata_block_size()
           ) do
      Encoder.decode_metadata_block(metadata_block)
    end
  end

  defp parse_bloom_filter(handler, {pos, size}) do
    with {:ok, bloom_filter_block} <- Handler.read(handler, pos, size) do
      Encoder.decode_bloom_filter_block(bloom_filter_block)
    end
  end

  defp parse_key_range(handler, {pos, size}) do
    with {:ok, key_range_block} <- Handler.read(handler, pos, size) do
      Encoder.decode_key_range_block(key_range_block)
    end
  end

  defp parse_seq_range(handler, {pos, size}) do
    with {:ok, seq_range_block} <- Handler.read(handler, pos, size) do
      Encoder.decode_seq_range_block(seq_range_block)
    end
  end
end

defimpl Goblin.Queryable, for: Goblin.DiskTables.DiskTable do
  alias Goblin.DiskTables.{DiskTable, BinarySearchIterator, StreamIterator}

  def has_key?(disk_table, key) do
    DiskTable.within_min_max?(disk_table, key) and
      DiskTable.bloom_filter_member?(disk_table, key)
  end

  def search(disk_table, keys, seq) do
    keys =
      Enum.filter(keys, fn key ->
        DiskTable.within_min_max?(disk_table, key) and
          DiskTable.bloom_filter_member?(disk_table, key)
      end)

    case keys do
      [] -> []
      _ -> BinarySearchIterator.new(disk_table, keys, seq)
    end
  end

  def stream(disk_table, min, max, seq) do
    case DiskTable.within_bounds?(disk_table, min, max) do
      true -> StreamIterator.new(disk_table, seq)
      false -> []
    end
  end
end
