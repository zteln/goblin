defmodule SeaGoat.SSTables do
  alias SeaGoat.BloomFilter
  alias SeaGoat.SSTables.Disk
  alias SeaGoat.SSTables.SSTable
  alias SeaGoat.SSTables.SSTableIterator

  def delete([]), do: :ok

  def delete([path | paths]) do
    with :ok = Disk.rm(path) do
      delete(paths)
    end
  end

  def switch(from, to) do
    :ok = Disk.rename(from, to)
  end

  def write(iterator, data, path, level) do
    with {:ok, bloom_filter} <- write_to_file(path, level, iterator, data) do
      {:ok, bloom_filter, path, level}
    end
  end

  def fetch_bloom_filter(file) do
    read_f = fn disk, metadata ->
      {level, bf_size, bf_pos, _, _, _, _} = metadata

      with {:ok, bf} <- fetch_part(disk, bf_pos, bf_size, &SSTable.decode_bloom_filter/1) do
        {:ok, {bf, level}}
      end
    end

    read = &fetch_metadata(&1, read_f)
    read_file(file, read)
  end

  def search_for_key(file, key) do
    read_f = fn disk, metadata ->
      {_, _, _, range_size, range_pos, no_of_blocks, _} = metadata

      case fetch_part(disk, range_pos, range_size, &SSTable.decode_range/1) do
        {:ok, {smallest, largest}} when key >= smallest and key <= largest ->
          {:ok, no_of_blocks}

        _ ->
          nil
      end
    end

    read = fn disk ->
      with {:ok, no_of_blocks} <- fetch_metadata(disk, read_f) do
        fetch_key(disk, 0, no_of_blocks, key)
      end
    end

    read_file(file, read)
  end

  defp write_to_file(path, level, iterator, data) do
    with {:ok, disk} <- Disk.open(path, write?: true),
         {:ok, disk, bloom_filter} <- write_ss_table(disk, level, iterator, data),
         :ok <- Disk.sync(disk),
         :ok <- Disk.close(disk) do
      {:ok, bloom_filter}
    end
  end

  defp write_ss_table(disk, level, iterator, data) do
    with {:ok, iterator} <- SSTableIterator.init(iterator, data),
         {:ok, disk, no_of_blocks, range, bloom_filter, iterator} <-
           write_data(disk, iterator),
         :ok <- SSTableIterator.deinit(iterator) do
      write_meta(disk, level, bloom_filter, range, no_of_blocks)
    end
  end

  defp write_data(
         disk,
         iterator,
         no_of_blocks \\ 0,
         {smallest, largest} \\ {nil, nil},
         bloom_filter \\ BloomFilter.new()
       ) do
    case SSTableIterator.next(iterator) do
      {:next, {k, v}, iterator} ->
        block = SSTable.encode_block(k, v)
        span = SSTable.span(byte_size(block))
        smallest = if smallest, do: smallest, else: k
        largest = k
        bloom_filter = BloomFilter.put(bloom_filter, k)
        {:ok, disk} = Disk.write(disk, block)

        write_data(
          disk,
          iterator,
          no_of_blocks + span,
          {smallest, largest},
          bloom_filter
        )

      {:eod, iterator} ->
        bloom_filter = BloomFilter.generate(bloom_filter)
        {:ok, disk, no_of_blocks, {smallest, largest}, bloom_filter, iterator}
    end
  end

  defp write_meta(disk, level, bloom_filter, range, no_of_blocks) do
    footer = SSTable.encode_footer(level, bloom_filter, range, disk.offset, no_of_blocks)

    with {:ok, disk} <- Disk.write(disk, footer) do
      {:ok, disk, bloom_filter}
    end
  end

  defp read_file(file, reader) do
    disk = Disk.open!(file)

    with {:ok, magic} <- Disk.read_from_end(disk, SSTable.size(:magic), SSTable.size(:magic)),
         true <- SSTable.is_ss_table(magic) do
      result = reader.(disk)
      Disk.close(disk)
      result
    else
      _ ->
        Disk.close(disk)
        {:error, :not_an_ss_table}
    end
  end

  defp fetch_metadata(disk, f) do
    with {:ok, encoded_metadata} <-
           Disk.read_from_end(
             disk,
             SSTable.size(:magic) + SSTable.size(:metadata),
             SSTable.size(:metadata)
           ),
         {:ok, metadata} <- SSTable.decode_metadata(encoded_metadata) do
      f.(disk, metadata)
    end
  end

  defp fetch_part(disk, position, size, decoder) do
    with {:ok, encoded} <- Disk.read(disk, position, size),
         {:ok, decoded} <- decoder.(encoded) do
      {:ok, decoded}
    end
  end

  defp fetch_key(_disk, low, high, _key) when high < low, do: :error

  defp fetch_key(disk, low, high, key) do
    mid = div(low + high, 2)
    position = (mid - 1) * SSTable.size(:block)

    with {:ok, k, v} <- get_block(disk, position) do
      cond do
        key < k -> fetch_key(disk, low, mid - 1, key)
        key > k -> fetch_key(disk, mid + 1, high, key)
        key == k -> {:ok, {:value, v}}
      end
    end
  end

  defp get_block(disk, position) do
    position = max(0, position)

    with {:ok, encoded_header} <- Disk.read(disk, position, SSTable.size(:block_header)),
         {:ok, span} <- SSTable.block_span(encoded_header),
         {:ok, encoded} <- Disk.read(disk, position, SSTable.size(:block) * span),
         {:ok, {key, value}} <- SSTable.decode_block(encoded) do
      {:ok, key, value}
    else
      {:error, :not_block_start} ->
        get_block(disk, position - SSTable.size(:block))

      e ->
        e
    end
  end
end
