defmodule Goblin.SSTs do
  @moduledoc false
  alias Goblin.SSTs.Disk
  alias Goblin.SSTs.SST
  alias Goblin.BloomFilter

  @tmp_suffix ".tmp"

  @type iterator :: {:next, Disk.t()}
  @type new_sst_opts :: [
          file_getter: (-> Goblin.db_file()),
          bf_fpp: number()
        ]

  @spec new([Enumerable.t(Goblin.triple())], Goblin.db_level_key(), new_sst_opts()) ::
          {:ok, [SST.t()]} | {:error, term()}
  def new(streams, level_key, opts) do
    with {:ok, new} <- write_streams(streams, level_key, opts) do
      switch(new)
    end
  end

  @spec delete(Goblin.db_file()) :: :ok | {:error, term()}
  def delete(file), do: Disk.rm(file)

  @spec find(Goblin.db_file(), Goblin.db_key()) ::
          {:ok, {:value, Goblin.seq_no(), Goblin.db_value()}} | :not_found | {:error, term()}
  def find(file, key) do
    with {:ok, disk} <- Disk.open(file) do
      search_result = search_sst(disk, key)
      Disk.close(disk)
      search_result
    end
  end

  @spec fetch_sst(Goblin.db_file()) :: {:ok, SST.t()} | {:error, term()}
  def fetch_sst(file) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_sst(disk),
           {:ok,
            {
              level_key,
              bf_pos,
              bf_size,
              key_range_pos,
              key_range_size,
              seq_range_pos,
              seq_range_size,
              _,
              size,
              offset,
              crc
            }} <- read_metadata(disk),
           :ok <- verify_crc(disk, 0, offset, crc),
           {:ok, bf} <- read_bloom_filter(disk, bf_pos, bf_size),
           {:ok, key_range} <- read_key_range(disk, key_range_pos, key_range_size),
           {:ok, seq_range} <- read_seq_range(disk, seq_range_pos, seq_range_size) do
        {:ok,
         %SST{
           file: file,
           bloom_filter: bf,
           level_key: level_key,
           seq_range: seq_range,
           key_range: key_range,
           size: size
         }}
      end

    Disk.close(disk)
    result
  end

  @spec stream!(Goblin.db_file()) :: Enumerable.t()
  def stream!(file) do
    Stream.resource(
      fn ->
        Disk.open!(file, start?: true)
      end,
      fn disk ->
        case read_next_key(disk) do
          {:ok, data, disk} ->
            {[data], disk}

          {:error, :eod} ->
            {:halt, disk}

          {:error, _reason} ->
            Disk.close(disk)
            raise "stream failed"
        end
      end,
      fn disk -> Disk.close(disk) end
    )
  end

  @spec iterate(iterator()) :: iterator() | {Goblin.triple(), iterator()} | :ok
  def iterate({:next, disk}) do
    case read_next_key(disk) do
      {:ok, data, disk} ->
        {data, {:next, disk}}

      {:error, :eod} ->
        Disk.close(disk)
        :ok

      _e ->
        Disk.close(disk)
        raise "Iteration failed."
    end
  end

  def iterator(file) do
    disk = Disk.open!(file, start?: true)
    {:next, disk}
  end

  defp write_streams(streams, level_key, opts, acc \\ [])
  defp write_streams([], _level_key, _opts, acc), do: {:ok, acc}

  defp write_streams([stream | rest], level_key, opts, acc) do
    with {:ok, new} <- write_stream(stream, level_key, opts) do
      write_streams(rest, level_key, opts, new ++ acc)
    end
  end

  defp write_stream(stream, level_key, opts) do
    file_getter = opts[:file_getter]

    Enum.reduce_while(stream, {:ok, []}, fn chunk, {:ok, acc} ->
      file = file_getter.()
      tmp_file = tmp_file(file)

      case write(tmp_file, level_key, chunk, opts) do
        {:ok, sst} ->
          {:cont, {:ok, [{tmp_file, file, sst} | acc]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp write(file, level_key, data, opts) do
    disk = Disk.open!(file, write?: true)

    result =
      with {:ok, _disk, bloom_filter, seq_range, size, key_range} <-
             write_sst(disk, level_key, data, opts) do
        {:ok,
         %SST{
           bloom_filter: bloom_filter,
           level_key: level_key,
           seq_range: seq_range,
           size: size,
           key_range: key_range
         }}
      end

    Disk.sync(disk)
    Disk.close(disk)
    result
  end

  defp search_sst(disk, key) do
    with :ok <- valid_sst(disk),
         {:ok, {_, _, _, _, _, _, _, no_of_blocks, _, _, _}} <- read_metadata(disk) do
      binary_search(disk, key, 0, no_of_blocks)
    end
  end

  defp valid_sst(disk) do
    with {:ok, magic} <- Disk.read_from_end(disk, SST.size(:magic), SST.size(:magic)),
         true <- SST.is_ss_table(magic) do
      :ok
    else
      _ ->
        {:error, :not_an_ss_table}
    end
  end

  defp read_metadata(disk) do
    with {:ok, encoded_metadata} <-
           Disk.read_from_end(
             disk,
             SST.size(:magic) + SST.size(:metadata),
             SST.size(:metadata)
           ) do
      SST.decode_metadata(encoded_metadata)
    end
  end

  defp read_bloom_filter(disk, pos, size) do
    with {:ok, encoded_bf} <- Disk.read(disk, pos, size) do
      SST.decode_bloom_filter(encoded_bf)
    end
  end

  defp read_key_range(disk, pos, size) do
    with {:ok, encoded_key_range} <- Disk.read(disk, pos, size) do
      SST.decode_key_range(encoded_key_range)
    end
  end

  defp read_seq_range(disk, pos, size) do
    with {:ok, encoded_seq_range} <- Disk.read(disk, pos, size) do
      SST.decode_seq_range(encoded_seq_range)
    end
  end

  defp binary_search(_disk, _key, low, high) when high < low, do: :not_found

  defp binary_search(disk, key, low, high) do
    mid = div(low + high, 2)
    position = (mid - 1) * SST.size(:block)

    with {:ok, k, seq, v} <- read_block(disk, position) do
      cond do
        key == k ->
          {:ok, {:value, seq, v}}

        key < k ->
          binary_search(disk, key, low, mid - 1)

        key > k ->
          binary_search(disk, key, mid + 1, high)
      end
    end
  end

  defp read_block(disk, position) do
    position = max(0, position)

    with {:ok, encoded_header} <- Disk.read(disk, position, SST.size(:block_header)),
         {:ok, span} <- SST.block_span(encoded_header),
         {:ok, encoded} <- Disk.read(disk, position, SST.size(:block) * span),
         {:ok, {key, seq, value}} <- SST.decode_block(encoded) do
      {:ok, key, seq, value}
    else
      {:error, :not_block_start} ->
        read_block(disk, position - SST.size(:block))

      e ->
        e
    end
  end

  defp read_next_key(disk) do
    with {:ok, enc_header} <- Disk.read(disk, SST.size(:block_header)),
         {:ok, span} <- SST.block_span(enc_header),
         {:ok, enc_block} <- Disk.read(disk, SST.size(:block) * span),
         {:ok, data} <- SST.decode_block(enc_block) do
      {:ok, data, Disk.advance_offset(disk, SST.size(:block) * span)}
    end
  end

  defp write_sst(disk, level_key, data, opts) do
    with {:ok, disk, data} <- write_data(disk, data, opts),
         {:ok, disk, bloom_filter, meta_size} <- write_meta(disk, level_key, data, opts) do
      {:ok, disk, bloom_filter, data.seq_range, data.size + meta_size, data.key_range}
    end
  end

  defp write_data(disk, data, opts) do
    compress? = opts[:compress?] || false

    init = %{
      no_of_blocks: 0,
      key_range: {nil, nil},
      seq_range: {nil, 0},
      bloom_filter: BloomFilter.new(),
      size: 0,
      crc: :erlang.crc32(<<>>)
    }

    Enum.reduce_while(data, {:ok, disk, init}, fn {k, seq, v}, {:ok, disk, acc} ->
      block = SST.encode_block(k, seq, v, compress?)
      block_size = byte_size(block)
      span = SST.span(block_size)
      {min_seq, max_seq} = acc.seq_range

      min_seq =
        if is_nil(min_seq) do
          seq
        else
          min(min_seq, seq)
        end

      max_seq = max(max_seq, seq)
      {min_key, _max_key} = acc.key_range
      min_key = if min_key, do: min_key, else: k
      max_key = k
      bloom_filter = BloomFilter.put(acc.bloom_filter, k)
      crc = :erlang.crc32(acc.crc, block)

      case Disk.write(disk, block) do
        {:ok, disk} ->
          acc = %{
            acc
            | no_of_blocks: acc.no_of_blocks + span,
              key_range: {min_key, max_key},
              seq_range: {min_seq, max_seq},
              bloom_filter: bloom_filter,
              size: acc.size + block_size,
              crc: crc
          }

          {:cont, {:ok, disk, acc}}

        error ->
          {:halt, error}
      end
    end)
  end

  defp write_meta(disk, level_key, data, opts) do
    bf_fpp = opts[:bf_fpp]
    compress? = opts[:compress?] || false
    bloom_filter = BloomFilter.generate(data.bloom_filter, bf_fpp)

    footer =
      SST.encode_footer(
        level_key,
        bloom_filter,
        data.key_range,
        data.seq_range,
        disk.offset,
        data.no_of_blocks,
        data.size,
        data.crc,
        compress?
      )

    with {:ok, disk} <- Disk.write(disk, footer) do
      {:ok, disk, bloom_filter, byte_size(footer)}
    end
  end

  defp switch(to_switch, acc \\ [])
  defp switch([], acc), do: {:ok, acc}

  defp switch([{from, to, sst} | to_switch], acc) do
    with :ok <- Disk.rename(from, to) do
      switch(to_switch, [%{sst | file: to} | acc])
    end
  end

  defp verify_crc(disk, start_pos, end_pos, target_crc, crc \\ :erlang.crc32(<<>>))

  defp verify_crc(_disk, end_pos, end_pos, target_crc, crc) do
    if target_crc == crc do
      :ok
    else
      {:error, :corrupted_sst}
    end
  end

  defp verify_crc(disk, start_pos, end_pos, target_crc, crc) do
    inc =
      if end_pos - start_pos < 4096 do
        end_pos - start_pos
      else
        4096
      end

    with {:ok, chunk} <- Disk.read(disk, start_pos, inc) do
      crc = :erlang.crc32(crc, chunk)
      verify_crc(disk, start_pos + inc, end_pos, target_crc, crc)
    end
  end

  defp tmp_file(file), do: file <> @tmp_suffix
end
