defmodule SeaGoat.SSTables do
  alias SeaGoat.SSTable.Disk
  alias SeaGoat.BloomFilter
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
    read_f = fn io, _offset, metadata ->
      {level, bf_size, bf_pos, _, _, _, _} = metadata

      with {:ok, bf} <- fetch_part(io, bf_pos, bf_size, &SSTable.decode_bloom_filter/1) do
        {:ok, {bf, level}}
      end
    end

    read = &fetch_metadata(&1, &2, read_f)
    read_file(file, read)
  end

  def search_for_key(file, key) do
    read_f = fn io, _offset, metadata ->
      {_, _, _, range_size, range_pos, no_of_blocks, _} = metadata

      case fetch_part(io, range_pos, range_size, &SSTable.decode_range/1) do
        {:ok, {smallest, largest}} when key >= smallest and key <= largest ->
          {:ok, no_of_blocks}

        _ ->
          nil
      end
    end

    read = fn io, offset ->
      with {:ok, no_of_blocks} <- fetch_metadata(io, offset, read_f) do
        fetch_key(io, 0, no_of_blocks, key)
      end
    end

    read_file(file, read)
  end

  defp write_to_file(path, level, iterator, data) do
    starting_offset = 0

    with {:ok, io, ^starting_offset} <- Disk.open(path, write?: true),
         {:ok, bloom_filter} <- write_ss_table(io, starting_offset, level, iterator, data),
         :ok <- Disk.sync(io),
         :ok <- Disk.close(io) do
      {:ok, bloom_filter}
    end
  end

  defp write_ss_table(io, offset, level, iterator, data) do
    with {:ok, iterator} <- SSTableIterator.init(iterator, data),
         {:ok, no_of_blocks, offset, range, bloom_filter, iterator} <-
           write_data(io, offset, iterator),
         :ok <- SSTableIterator.deinit(iterator) do
      write_meta(io, level, bloom_filter, range, offset, no_of_blocks)
    end
  end

  defp write_data(
         io,
         offset,
         iterator,
         no_of_blocks \\ 0,
         {smallest, largest} \\ {nil, nil},
         bloom_filter \\ BloomFilter.new()
       ) do
    case SSTableIterator.next(iterator) do
      {:next, {k, v}, iterator} ->
        {:ok, offset} = Disk.write(io, offset, SSTable.encode_block(k, v))
        smallest = if smallest, do: smallest, else: k
        largest = k
        bloom_filter = BloomFilter.put(bloom_filter, k)
        write_data(io, offset, iterator, no_of_blocks + 1, {smallest, largest}, bloom_filter)

      {:eod, iterator} ->
        bloom_filter = BloomFilter.generate(bloom_filter)
        {:ok, no_of_blocks, offset, {smallest, largest}, bloom_filter, iterator}
    end
  end

  defp write_meta(io, level, bloom_filter, range, offset, no_of_blocks) do
    footer = SSTable.encode_footer(level, bloom_filter, range, offset, no_of_blocks)

    with {:ok, _offset} <- Disk.write(io, offset, footer) do
      {:ok, bloom_filter}
    end
  end

  defp read_file(file, reader) do
    {io, offset} = Disk.open!(file)

    with {:ok, magic} <- Disk.read(io, offset - SSTable.size(:magic), SSTable.size(:magic)),
         true <- SSTable.is_ss_table(magic) do
      result = reader.(io, offset)
      Disk.close(io)
      result
    else
      _ ->
        Disk.close(io)
        {:error, :not_an_ss_table}
    end
  end

  defp fetch_metadata(io, offset, f) do
    with {:ok, encoded_metadata} <-
           Disk.read(
             io,
             offset - SSTable.size(:magic) - SSTable.size(:metadata),
             SSTable.size(:metadata)
           ),
         {:ok, metadata} <- SSTable.decode_metadata(encoded_metadata) do
      f.(io, offset, metadata)
    end
  end

  defp fetch_part(io, offset, size, decoder) do
    with {:ok, encoded} <- Disk.read(io, offset, size),
         {:ok, decoded} <- decoder.(encoded) do
      {:ok, decoded}
    end
  end

  defp fetch_key(io, low, high, key) do
    mid = div(low + high, 2)
    offset = (mid - 1) * SSTable.size(:block)

    with {:ok, k, v} <- get_block(io, offset) do
      cond do
        key < k -> fetch_key(io, low, mid - 1, key)
        key > k -> fetch_key(io, mid + 1, high, key)
        key == k -> {:ok, {:value, v}}
      end
    end
  end

  defp get_block(io, offset) do
    with {:ok, encoded_header} <- Disk.read(io, offset, SSTable.size(:block_header)),
         {:ok, span} <- SSTable.block_span(encoded_header),
         {:ok, encoded} <- Disk.read(io, offset, SSTable.size(:block) * span),
         {:ok, {key, value}} <- SSTable.decode_block(encoded) do
      {:ok, key, value}
    else
      {:error, :not_block_start} ->
        get_block(io, offset - SSTable.size(:block))

      e ->
        e
    end
  end
end
