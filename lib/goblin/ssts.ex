defmodule Goblin.SSTs do
  @moduledoc false
  alias Goblin.SSTs.Disk
  alias Goblin.SSTs.SST
  alias Goblin.BloomFilter

  @tmp_suffix ".tmp"

  @type iterator :: {:next, Disk.t()}

  @spec new([Enumerable.t(Goblin.triple())], Goblin.level_key(), (-> Goblin.db_file())) ::
          {:ok, [SST.t()]} | {:error, term()}
  def new(streams, level_key, file_getter) do
    with {:ok, new} <- write_streams(streams, level_key, file_getter) do
      switch(new)
    end
  end

  @spec delete(Goblin.db_file()) :: :ok | {:error, term()}
  def delete(file), do: Disk.rm(file)

  @spec find(Goblin.db_file(), Goblin.db_key()) ::
          {:ok, {:value, Goblin.db_sequence(), Goblin.db_value()}} | :not_found | {:error, term()}
  def find(file, key) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_ss_table(disk),
           {:ok, {_, _, _, _, _, _, _, no_of_blocks, _, _}} <- read_metadata(disk) do
        binary_search(disk, key, 0, no_of_blocks)
      end

    Disk.close(disk)
    result
  end

  @spec fetch_sst(Goblin.db_file()) ::
          {:ok, {Goblin.BloomFilter.t(), non_neg_integer()}} | {:error, term()}
  def fetch_sst(file) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_ss_table(disk),
           {:ok,
            {
              level_key,
              bf_pos,
              bf_size,
              key_range_pos,
              key_range_size,
              priority_pos,
              priority_size,
              _,
              size,
              _
            }} <- read_metadata(disk),
           {:ok, bf} <- read_bloom_filter(disk, bf_pos, bf_size),
           {:ok, key_range} <- read_key_range(disk, key_range_pos, key_range_size),
           {:ok, priority} <- read_priority(disk, priority_pos, priority_size) do
        {:ok,
         %SST{
           file: file,
           bloom_filter: bf,
           level_key: level_key,
           priority: priority,
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

  @spec iterate(Goblin.db_file() | iterator()) :: iterator()
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

  def iterate(file) do
    disk = Disk.open!(file, start?: true)
    {:next, disk}
  end

  defp write_streams(streams, level_key, file_getter, acc \\ [])
  defp write_streams([], _level_key, _file_getter, acc), do: {:ok, acc}

  defp write_streams([stream | rest], level_key, file_getter, acc) do
    with {:ok, new} <- write_stream(stream, level_key, file_getter) do
      write_streams(rest, level_key, file_getter, new ++ acc)
    end
  end

  defp write_stream(stream, level_key, file_getter) do
    Enum.reduce_while(stream, {:ok, []}, fn chunk, {:ok, acc} ->
      file = file_getter.()
      tmp_file = tmp_file(file)

      case write(tmp_file, level_key, chunk) do
        {:ok, sst} ->
          {:cont, {:ok, [{tmp_file, file, sst} | acc]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp write(file, level_key, data) do
    disk = Disk.open!(file, write?: true)

    result =
      with {:ok, _disk, bloom_filter, priority, size, key_range} <-
             write_sst(disk, level_key, data) do
        {:ok,
         %SST{
           bloom_filter: bloom_filter,
           level_key: level_key,
           priority: priority,
           size: size,
           key_range: key_range
         }}
      end

    Disk.sync(disk)
    Disk.close(disk)
    result
  end

  defp valid_ss_table(disk) do
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

  defp read_priority(disk, pos, size) do
    with {:ok, encoded_priority} <- Disk.read(disk, pos, size) do
      SST.decode_priority(encoded_priority)
    end
  end

  defp binary_search(_disk, _key, low, high) when high < low, do: :not_found

  defp binary_search(disk, key, low, high) do
    mid = div(low + high, 2)
    position = (mid - 1) * SST.size(:block)

    with {:ok, seq, k, v} <- read_block(disk, position) do
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
         {:ok, {seq, key, value}} <- SST.decode_block(encoded) do
      {:ok, seq, key, value}
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

  defp write_sst(disk, level_key, data) do
    with {:ok, disk, data} <- write_data(disk, data),
         {:ok, disk, bloom_filter, meta_size} <- write_meta(disk, level_key, data) do
      {:ok, disk, bloom_filter, data.priority, data.size + meta_size, data.key_range}
    end
  end

  defp write_data(disk, data) do
    init = %{
      priority: nil,
      no_of_keys: 0,
      no_of_blocks: 0,
      key_range: {nil, nil},
      bloom_filter: BloomFilter.new(),
      size: 0
    }

    Enum.reduce_while(data, {:ok, disk, init}, fn {seq, k, v}, {:ok, disk, acc} ->
      block = SST.encode_block(seq, k, v)
      block_size = byte_size(block)
      span = SST.span(block_size)
      priority = if acc.priority, do: acc.priority, else: seq
      {smallest, _largest} = acc.key_range
      smallest = if smallest, do: smallest, else: k
      largest = k
      bloom_filter = BloomFilter.put(acc.bloom_filter, k)

      case Disk.write(disk, block) do
        {:ok, disk} ->
          acc = %{
            acc
            | no_of_keys: acc.no_of_keys + 1,
              no_of_blocks: acc.no_of_blocks + span,
              key_range: {smallest, largest},
              priority: priority,
              bloom_filter: bloom_filter,
              size: acc.size + block_size
          }

          {:cont, {:ok, disk, acc}}

        error ->
          {:halt, error}
      end
    end)
  end

  defp write_meta(disk, level_key, data) do
    bloom_filter = BloomFilter.generate(data.bloom_filter)

    footer =
      SST.encode_footer(
        level_key,
        bloom_filter,
        data.key_range,
        data.priority,
        disk.offset,
        data.no_of_keys,
        data.no_of_blocks
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

  defp tmp_file(file), do: file <> @tmp_suffix
end
