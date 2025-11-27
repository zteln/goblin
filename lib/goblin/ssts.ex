defmodule Goblin.SSTs do
  @moduledoc false
  alias Goblin.SSTs.Disk
  alias Goblin.SSTs.SST
  alias Goblin.BloomFilter

  @tmp_suffix ".tmp"

  @spec new(Enumerable.t(Goblin.triple()), keyword()) :: {:ok, [SST.t()]} | {:error, term()}
  def new(data, opts) do
    file = opts[:file_getter].()
    tmp_file = tmp_file(file)
    acc = new_write_acc(tmp_file, file)
    moves = [{tmp_file, file}]
    ssts = []

    with {:ok, acc, moves, ssts} <-
           Enum.reduce_while(
             data,
             {:ok, acc, moves, ssts},
             &sst_write_reducer(&1, &2, opts[:max_sst_size], opts)
           ),
         {:ok, sst} <- append_metadata(acc, opts),
         :ok <- switch(moves) do
      {:ok, Enum.reverse([sst | ssts])}
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

  @spec iterate(Disk.t()) :: {Goblin.triple(), Disk.t()} | :ok
  def iterate(disk) do
    case read_next_key(disk) do
      {:ok, data, disk} ->
        {data, disk}

      {:error, :eod} ->
        # Disk.close(disk)
        :ok

      _e ->
        Disk.close(disk)
        raise "Iteration failed."
    end
  end

  @spec iterator(Goblin.db_file()) :: Goblin.Iterator.iterator()
  def iterator(file) do
    disk = Disk.open!(file, start?: true)
    {disk, &iterate/1, &Disk.close/1}
  end

  defp new_write_acc(tmp_file, file) do
    %{
      file: file,
      disk: Disk.open!(tmp_file, write?: true),
      size: 0,
      no_of_blocks: 0,
      key_range: {nil, nil},
      seq_range: {nil, 0},
      bloom_filter: BloomFilter.new(),
      crc: :erlang.crc32(<<>>)
    }
  end

  defp sst_write_reducer(triple, {:ok, %{size: size} = acc, moves, ssts}, max_sst_size, opts)
       when size >= max_sst_size do
    file = opts[:file_getter].()
    tmp_file = tmp_file(file)
    new_acc = new_write_acc(tmp_file, file)

    with {:ok, sst} <- append_metadata(acc, opts),
         {:ok, acc} <- append_data(triple, new_acc, opts) do
      {:cont, {:ok, acc, [{tmp_file, file} | moves], [sst | ssts]}}
    else
      error -> {:halt, error}
    end
  end

  defp sst_write_reducer(triple, {:ok, acc, moves, ssts}, _max_sst_size, opts) do
    case append_data(triple, acc, opts) do
      {:ok, acc} -> {:cont, {:ok, acc, moves, ssts}}
      error -> {:halt, error}
    end
  end

  defp append_data({k, seq, v}, acc, opts) do
    compress? = opts[:compress?] || false
    block = SST.encode_block(k, seq, v, compress?)
    block_size = byte_size(block)
    span = SST.span(block_size)
    {min_seq, max_seq} = acc.seq_range
    min_seq = if is_nil(min_seq), do: seq, else: min(min_seq, seq)
    max_seq = max(max_seq, seq)
    {min_key, _max_key} = acc.key_range
    min_key = if min_key, do: min_key, else: k
    max_key = k
    bloom_filter = BloomFilter.put(acc.bloom_filter, k)
    crc = :erlang.crc32(acc.crc, block)

    with {:ok, disk} <- Disk.write(acc.disk, block) do
      acc = %{
        acc
        | no_of_blocks: acc.no_of_blocks + span,
          key_range: {min_key, max_key},
          seq_range: {min_seq, max_seq},
          bloom_filter: bloom_filter,
          size: acc.size + block_size,
          crc: crc,
          disk: disk
      }

      {:ok, acc}
    end
  end

  defp append_metadata(acc, opts) do
    bf = BloomFilter.generate(acc.bloom_filter, opts[:bf_fpp])

    footer =
      SST.encode_footer(
        opts[:level_key],
        bf,
        acc.key_range,
        acc.seq_range,
        acc.disk.offset,
        acc.no_of_blocks,
        acc.size,
        acc.crc,
        opts[:compress?] || false
      )

    with {:ok, disk} <- Disk.write(acc.disk, footer),
         :ok <- Disk.close(disk) do
      {:ok,
       %SST{
         file: acc.file,
         level_key: opts[:level_key],
         bloom_filter: bf,
         seq_range: acc.seq_range,
         key_range: acc.key_range,
         size: acc.size + byte_size(footer)
       }}
    end
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

  defp switch([]), do: :ok

  defp switch([{from, to} | to_switch]) do
    with :ok <- Disk.rename(from, to) do
      switch(to_switch)
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
