defmodule Goblin.DiskTables.DiskTable do
  @moduledoc false
  alias Goblin.BloomFilter
  alias Goblin.DiskTables.{Encoder, Handler}

  defstruct [
    :file,
    :level_key,
    bloom_filter: BloomFilter.new(),
    key_range: {nil, nil},
    seq_range: {nil, 0},
    size: 0,
    no_blocks: 0,
    crc: :erlang.crc32(<<>>)
  ]

  @type t :: %__MODULE__{}

  @spec write_new(
          Enumerable.t(Goblin.triple()),
          (-> {Path.t(), Path.t()}),
          keyword()
        ) :: {:ok, [t()]} | {:error, term()}
  def write_new(data_stream, next_file_f, opts) do
    level_key = opts[:level_key]
    compress? = opts[:compress?]
    max_sst_size = opts[:max_sst_size]
    bf_fpp = opts[:bf_fpp]

    Enum.reduce_while(data_stream, {nil, nil, [], []}, fn
      triple, {nil, nil, files, disk_tables} ->
        {tmp_file, file} = new_files = next_file_f.()
        disk_table = %__MODULE__{file: file, level_key: level_key}

        with {:ok, handler} <- Handler.open(tmp_file, write?: true),
             {:ok, disk_table, handler} <-
               append_sst_block(disk_table, handler, triple, compress?) do
          {:cont, {disk_table, handler, [new_files | files], disk_tables}}
        else
          error -> {:halt, error}
        end

      triple, {disk_table, handler, files, disk_tables} ->
        case append_sst_block(disk_table, handler, triple, compress?) do
          {:ok, %{size: size} = disk_table, handler} when size >= max_sst_size ->
            case append_footer_and_close(disk_table, handler, bf_fpp, compress?) do
              {:ok, disk_table} -> {:cont, {nil, nil, files, [disk_table | disk_tables]}}
              error -> {:halt, error}
            end

          {:ok, disk_table, handler} ->
            {:cont, {disk_table, handler, files, disk_tables}}

          error ->
            {:halt, error}
        end
    end)
    |> then(fn
      {:error, _} = error ->
        error

      {nil, nil, files, disk_tables} ->
        with :ok <- mv_files(files) do
          {:ok, Enum.reverse(disk_tables)}
        end

      {disk_table, handler, files, disk_tables} ->
        with {:ok, disk_table} <- append_footer_and_close(disk_table, handler, bf_fpp, compress?),
             :ok <- mv_files(files) do
          {:ok, Enum.reverse([disk_table | disk_tables])}
        end
    end)
  end

  @spec parse(Path.t()) :: {:ok, t()} | {:error, term()}
  def parse(file) do
    with {:ok, handler} <- Handler.open(file),
         :ok <- verify_magic(handler),
         {:ok, size} <- parse_size(handler),
         {:ok, crc} <- parse_crc(handler),
         :ok <- verify_crc(handler, size, crc),
         {:ok,
          {
            level_key,
            bf_block_info,
            key_range_block_info,
            seq_range_block_info,
            no_blocks
          }} <- parse_metadata(handler),
         {:ok, bloom_filter} <- parse_bloom_filter(handler, bf_block_info),
         {:ok, key_range} <- parse_key_range(handler, key_range_block_info),
         {:ok, seq_range} <- parse_seq_range(handler, seq_range_block_info),
         :ok <- Handler.close(handler) do
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
  end

  defp append_sst_block(disk_table, handler, {key, seq, _value} = triple, compress?) do
    {sst_block, no_blocks} = Encoder.encode_sst_block(triple, compress?)

    with {:ok, handler} <- Handler.write(handler, sst_block) do
      disk_table =
        disk_table
        |> update_key_range(key)
        |> update_seq_range(seq)
        |> update_bloom_filter(key)
        |> update_crc(Encoder.update_crc(disk_table.crc, sst_block))
        |> update_no_blocks(no_blocks)
        |> update_size(byte_size(sst_block))

      {:ok, disk_table, handler}
    end
  end

  defp append_footer(disk_table, handler, bf_fpp, compress?) do
    bloom_filter = BloomFilter.generate(disk_table.bloom_filter, bf_fpp)

    {footer_block, size, crc} =
      Encoder.encode_footer_block(
        disk_table.level_key,
        bloom_filter,
        disk_table.key_range,
        disk_table.seq_range,
        handler.file_offset,
        disk_table.no_blocks,
        disk_table.crc,
        disk_table.size,
        compress?
      )

    with {:ok, handler} <- Handler.write(handler, footer_block) do
      disk_table =
        disk_table
        |> set_bloom_filter(bloom_filter)
        |> set_size(size)
        |> update_crc(crc)

      {:ok, disk_table, handler}
    end
  end

  defp append_footer_and_close(disk_table, handler, bf_fpp, compress?) do
    with {:ok, disk_table, handler} <- append_footer(disk_table, handler, bf_fpp, compress?),
         :ok <- Handler.sync(handler),
         :ok <- Handler.close(handler) do
      {:ok, disk_table}
    end
  end

  defp mv_files([]), do: :ok

  defp mv_files([{from, to} | files]) do
    with :ok <- File.rename(from, to) do
      mv_files(files)
    end
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
    inc = min(finish - start, 4096)

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

  defp update_key_range(%{key_range: {nil, nil}} = disk_table, key) do
    %{disk_table | key_range: {key, key}}
  end

  defp update_key_range(disk_table, key) do
    %{key_range: {min_key, _}} = disk_table
    %{disk_table | key_range: {min_key, key}}
  end

  defp update_seq_range(disk_table, seq) do
    %{seq_range: {min_seq, max_seq}} = disk_table
    min_seq = if is_nil(min_seq), do: seq, else: min(min_seq, seq)
    max_seq = max(max_seq, seq)
    %{disk_table | seq_range: {min_seq, max_seq}}
  end

  defp update_bloom_filter(disk_table, key) do
    %{disk_table | bloom_filter: BloomFilter.put(disk_table.bloom_filter, key)}
  end

  defp set_bloom_filter(disk_table, bloom_filter) do
    %{disk_table | bloom_filter: bloom_filter}
  end

  defp update_crc(disk_table, crc) do
    %{disk_table | crc: crc}
  end

  defp update_no_blocks(disk_table, no_blocks) do
    %{disk_table | no_blocks: disk_table.no_blocks + no_blocks}
  end

  defp update_size(disk_table, size) do
    %{disk_table | size: disk_table.size + size}
  end

  defp set_size(disk_table, size) do
    %{disk_table | size: size}
  end
end
