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

  def write(iterator, data, path, tier) do
    with {:ok, bloom_filter} <- write_to_file(path, tier, iterator, data) do
      {:ok, bloom_filter, path, tier}
    end
  end

  def read_bloom_filter(file) do
    read_f = fn io, offset, footer ->
      tier = SSTable.footer_part(footer, :level)

      # TODO: Check if DB file first
      case read_meta_part(io, offset, footer, :bloom_filter) do
        {:ok, bloom_filter} ->
          {:ok, {bloom_filter, tier}}

        e ->
          e
      end
    end

    read = &read_meta(&1, &2, read_f)
    read_file(file, read)
  end

  def search_for_key(file, key) do
    read_f = fn io, offset, footer ->
      with {:ok, {smallest, largest}} when key >= smallest and key <= largest <-
             read_meta_part(io, offset, footer, :range),
           {:ok, index} <- read_meta_part(io, offset, footer, :index) do
        {:ok, Map.get(index, key)}
      else
        _ -> nil
      end
    end

    read = fn io, offset ->
      with {:ok, {key_offset, size}} <- read_meta(io, offset, read_f) do
        read_data(io, key_offset, size, key)
      end
    end

    read_file(file, read)
  end

  defp write_to_file(path, tier, iterator, data) do
    starting_offset = 0

    with {:ok, io, ^starting_offset} <- Disk.open(path, write?: true),
         {:ok, bloom_filter} <- write(io, starting_offset, tier, iterator, data),
         :ok <- Disk.sync(io),
         :ok <- Disk.close(io) do
      {:ok, bloom_filter}
    end
  end

  defp write(io, offset, tier, iterator, data) do
    with {:ok, iterator} <- SSTableIterator.init(iterator, data),
         {:ok, offset} <- Disk.write(io, offset, SSTable.new()),
         {:ok, index, offset, iterator} <- write_data(io, offset, iterator),
         :ok <- SSTableIterator.deinit(iterator) do
      write_meta(io, offset, index, tier)
    end
  end

  defp write_data(io, offset, iterator, index \\ %{}) do
    case SSTableIterator.next(iterator) do
      {:next, {k, v}, iterator} ->
        block = SSTable.encode(:data, key: k, value: v)
        {:ok, new_offset} = Disk.write(io, offset, block)
        index = Map.put(index, k, {offset, byte_size(block)})
        write_data(io, new_offset, iterator, index)

      {:eod, iterator} ->
        {:ok, index, offset, iterator}
    end
  end

  defp write_meta(io, offset, index, tier) do
    keys = Map.keys(index)
    range = {Enum.min(keys), Enum.max(index)}
    bloom_filter = BloomFilter.new(keys)

    meta =
      SSTable.encode(:meta, level: tier, range: range, index: index, bloom_filter: bloom_filter)

    with {:ok, _offset} <- Disk.write(io, offset, meta) do
      {:ok, bloom_filter}
    end
  end

  defp read_file(file, f) do
    {io, offset} = Disk.open!(file)

    case f.(io, offset) do
      {:ok, data} ->
        Disk.close(io)
        {:ok, data}

      e ->
        Disk.close(io)
        e
    end
  end

  defp read_meta(io, offset, f) do
    with {:ok, encoded_footer} <-
           Disk.read(io, SSTable.offset_calc(offset, :footer), SSTable.size(:footer)),
         {:ok, footer} <- SSTable.decode(:footer, encoded_footer) do
      f.(io, offset, footer)
    end
  end

  defp read_meta_part(io, offset, footer, meta_part_key) do
    with {:ok, encoded} <-
           Disk.read(
             io,
             SSTable.offset_calc(offset, footer, meta_part_key),
             SSTable.footer_part(footer, meta_part_key)
           ),
         {:ok, decoded} <- SSTable.decode(:meta_part, encoded) do
      {:ok, decoded}
    end
  end

  defp read_data(io, key_offset, size, key) do
    with {:ok, encoded} <- Disk.read(io, key_offset, size),
         {:ok, {fetched_key, value}} <- SSTable.decode(:data, encoded) do
      if fetched_key == key do
        value = if value == :tombstone, do: nil, else: value
        {:ok, {:value, value}}
      else
        {:error, :next}
      end
    end
  end
end
