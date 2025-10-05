defmodule SeaGoat.SSTables.Util do
  alias SeaGoat.SSTables.SSTable
  alias SeaGoat.SSTables.Disk
  alias SeaGoat.SSTables.Iterator
  alias SeaGoat.BloomFilter

  def valid_ss_table(disk) do
    with {:ok, magic} <- Disk.read_from_end(disk, SSTable.size(:magic), SSTable.size(:magic)),
         true <- SSTable.is_ss_table(magic) do
      :ok
    else
      _ ->
        {:error, :not_an_ss_table}
    end
  end

  def read_metadata(disk) do
    with {:ok, encoded_metadata} <-
           Disk.read_from_end(
             disk,
             SSTable.size(:magic) + SSTable.size(:metadata),
             SSTable.size(:metadata)
           ) do
      SSTable.decode_metadata(encoded_metadata)
    end
  end

  def read_bloom_filter(disk, pos, size) do
    with {:ok, encoded_bf} <- Disk.read(disk, pos, size) do
      SSTable.decode_bloom_filter(encoded_bf)
    end
  end

  def read_range(disk, pos, size) do
    with {:ok, encoded_range} <- Disk.read(disk, pos, size) do
      SSTable.decode_range(encoded_range)
    end
  end

  def read_sequence(disk, pos, size) do
    with {:ok, encoded_sequence} <- Disk.read(disk, pos, size) do
      SSTable.decode_sequence(encoded_sequence)
    end
  end

  def binary_search(_disk, _key, low, high) when high < low, do: :error

  def binary_search(disk, key, low, high) do
    mid = div(low + high, 2)
    position = (mid - 1) * SSTable.size(:block)

    with {:ok, k, v} <- read_block(disk, position) do
      cond do
        key == k ->
          {:ok, {:value, v}}

        key < k ->
          binary_search(disk, key, low, mid - 1)

        key > k ->
          binary_search(disk, key, mid + 1, high)
      end
    end
  end

  def read_block(disk, position) do
    position = max(0, position)

    with {:ok, encoded_header} <- Disk.read(disk, position, SSTable.size(:block_header)),
         {:ok, span} <- SSTable.block_span(encoded_header),
         {:ok, encoded} <- Disk.read(disk, position, SSTable.size(:block) * span),
         {:ok, {key, value}} <- SSTable.decode_block(encoded) do
      {:ok, key, value}
    else
      {:error, :not_block_start} ->
        read_block(disk, position - SSTable.size(:block))

      e ->
        e
    end
  end

  def write_to_file(file, level, iterator) do
    with {:ok, disk} <- Disk.open(file, write?: true),
         {:ok, disk, bloom_filter} <- write_ss_table(disk, level, iterator),
         :ok <- Disk.sync(disk),
         :ok <- Disk.close(disk) do
      {:ok, bloom_filter}
    end
  end

  def write_ss_table(disk, level, iterator) do
    with {:ok, iterator} <- Iterator.init(iterator),
         {:ok, disk, no_of_blocks, range, bloom_filter, min_seq, max_seq, iterator} <-
           write_data(disk, iterator),
         :ok <- Iterator.deinit(iterator) do
      write_meta(disk, level, bloom_filter, min_seq, max_seq, range, no_of_blocks)
    end
  end

  def write_data(
        disk,
        iterator,
        no_of_blocks \\ 0,
        {smallest, largest} \\ {nil, nil},
        bloom_filter \\ BloomFilter.new()
      ) do
    case Iterator.next(iterator) do
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

      {:end_iter, min_seq, max_seq, iterator} ->
        bloom_filter = BloomFilter.generate(bloom_filter)
        {:ok, disk, no_of_blocks, {smallest, largest}, bloom_filter, min_seq, max_seq, iterator}
    end
  end

  def write_meta(disk, level, bloom_filter, min_seq, max_seq, range, no_of_blocks) do
    footer =
      SSTable.encode_footer(
        level,
        bloom_filter,
        range,
        min_seq,
        max_seq,
        disk.offset,
        no_of_blocks
      )

    with {:ok, disk} <- Disk.write(disk, footer) do
      {:ok, disk, bloom_filter}
    end
  end

  def key_in_range({smallest, largest}, key) when key >= smallest and key <= largest, do: :ok
  def key_in_range(_, _), do: :error
end
