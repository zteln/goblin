defmodule SeaGoat.SSTables.Util do
  alias SeaGoat.SSTables.SSTable
  alias SeaGoat.SSTables.Disk
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

  def read_key_range(disk, pos, size) do
    with {:ok, encoded_key_range} <- Disk.read(disk, pos, size) do
      SSTable.decode_key_range(encoded_key_range)
    end
  end

  def read_priority(disk, pos, size) do
    with {:ok, encoded_priority} <- Disk.read(disk, pos, size) do
      SSTable.decode_priority(encoded_priority)
    end
  end

  def binary_search(_disk, _key, low, high) when high < low, do: :error

  def binary_search(disk, key, low, high) do
    mid = div(low + high, 2)
    position = (mid - 1) * SSTable.size(:block)

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

  def read_block(disk, position) do
    position = max(0, position)

    with {:ok, encoded_header} <- Disk.read(disk, position, SSTable.size(:block_header)),
         {:ok, span} <- SSTable.block_span(encoded_header),
         {:ok, encoded} <- Disk.read(disk, position, SSTable.size(:block) * span),
         {:ok, {seq, key, value}} <- SSTable.decode_block(encoded) do
      {:ok, seq, key, value}
    else
      {:error, :not_block_start} ->
        read_block(disk, position - SSTable.size(:block))

      e ->
        e
    end
  end

  def read_next_key(disk) do
    with {:ok, enc_header} <- Disk.read(disk, SSTable.size(:block_header)),
         {:ok, span} <- SSTable.block_span(enc_header),
         {:ok, enc_block} <- Disk.read(disk, SSTable.size(:block) * span),
         {:ok, data} <- SSTable.decode_block(enc_block) do
      {:ok, data, Disk.advance_offset(disk, SSTable.size(:block) * span)}
    end
  end

  def write_sst(disk, level_key, data) do
    with {:ok, disk, data} <- write_data(disk, data),
         {:ok, disk, bloom_filter, meta_size} <- write_meta(disk, level_key, data) do
      {:ok, disk, bloom_filter, data.priority, data.size + meta_size, data.key_range}
    end
  end

  def write_data(disk, data) do
    init = %{
      priority: nil,
      no_of_keys: 0,
      no_of_blocks: 0,
      key_range: {nil, nil},
      bloom_filter: BloomFilter.new(),
      size: 0
    }

    Enum.reduce_while(data, {:ok, disk, init}, fn {seq, k, v}, {:ok, disk, acc} ->
      block = SSTable.encode_block(seq, k, v)
      block_size = byte_size(block)
      span = SSTable.span(block_size)
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

  def write_meta(disk, level_key, data) do
    bloom_filter = BloomFilter.generate(data.bloom_filter)

    footer =
      SSTable.encode_footer(
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

  def key_in_range({smallest, largest}, key) when key >= smallest and key <= largest, do: :ok
  def key_in_range(_, _), do: :error
end
