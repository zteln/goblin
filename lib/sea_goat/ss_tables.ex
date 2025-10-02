defmodule SeaGoat.SSTables do
  @moduledoc """
  Provides operations for managing SSTable files on disk.

  SSTables (Sorted String Tables) are immutable, on-disk data structures used for 
  efficient storage and retrieval of key-value pairs. This module handles the 
  complete lifecycle of SSTable files including creation, reading, file management,
  and key lookups.
  """
  alias SeaGoat.BloomFilter
  alias SeaGoat.SSTables.Disk
  alias SeaGoat.SSTables.SSTable
  alias SeaGoat.SSTables.Iterator

  @doc """
  Deletes multiple SSTable files from disk.

  Attempts to delete each file in the provided list sequentially. If any 
  deletion fails, returns the first error encountered and stops processing
  remaining files.

  ## Parameters

  - `files` - List of files to delete

  ## Returns

  `:ok` if all files deleted successfully, or `{:error, reason}` on first failure.
  """
  @spec delete([SeaGoat.db_file()]) :: :ok | {:error, term()}
  def delete([]), do: :ok

  def delete([file | files]) do
    with :ok <- Disk.rm(file) do
      delete(files)
    end
  end

  @doc """
  Renames an SSTable file from one file to another.

  Atomically moves an SSTable file from the source file to the destination file.
  Commonly used during compaction operations to replace old SSTable files.

  ## Parameters

  - `from` - Source file
  - `to` - Destination file

  ## Returns

  `:ok` on success.
  """
  @spec switch(SeaGoat.db_file(), SeaGoat.db_file()) :: :ok
  def switch(from, to) do
    Disk.rename(from, to)
  end

  @doc """
  Writes key-value data to an SSTable file on disk.

  Creates a new SSTable file containing the data provided through the iterator. 
  The iterator determines how data is sourced and ordered:

  - `MemTableIterator` - sorts in-memory data before writing
  - `SSTablesIterator` - merges multiple existing SSTables in sorted order

  Returns the bloom filter, file, and level upon successful completion.

  ## Parameters

  - `iterator` - Iterator struct that implements the Iterator protocol
  - `data` - Data source passed to the iterator (mem_table, files, etc.)
  - `file` - File where the SSTable will be created
  - `level` - SSTable level for LSM tree organization

  ## Returns

  `{:ok, bloom_filter, file, level}` on success, or `{:error, reason}` on failure.
  """
  @spec write(Iterator.t(), Enumerable.t(), SeaGoat.db_file(), non_neg_integer()) ::
          {:ok, BloomFilter.t(), SeaGoat.db_file(), non_neg_integer()}
  def write(iterator, data, file, level) do
    with {:ok, bloom_filter} <- write_to_file(file, level, iterator, data) do
      {:ok, bloom_filter, file, level}
    end
  end

  defp write_to_file(file, level, iterator, data) do
    with {:ok, disk} <- Disk.open(file, write?: true),
         {:ok, disk, bloom_filter} <- write_ss_table(disk, level, iterator, data),
         :ok <- Disk.sync(disk),
         :ok <- Disk.close(disk) do
      {:ok, bloom_filter}
    end
  end

  defp write_ss_table(disk, level, iterator, data) do
    with {:ok, iterator} <- Iterator.init(iterator, data),
         {:ok, disk, no_of_blocks, range, bloom_filter, iterator} <-
           write_data(disk, iterator),
         :ok <- Iterator.deinit(iterator) do
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

      {:end_iter, iterator} ->
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

  @doc """
  Searches for a specific key in an SSTable file.

  Performs an efficient key lookup using the SSTable's range metadata and 
  binary search through the sorted blocks. Returns the associated value if
  the key is found.

  ## Parameters

  - `file` - Path to the SSTable file
  - `key` - The key to search for

  ## Returns

  `{:ok, {:value, value}}` if key found, `:error` if not found, or 
  `{:error, reason}` on file/parsing errors.
  """
  @spec read(SeaGoat.db_file(), SeaGoat.db_key()) :: {:ok, term()} | :error | {:error, term()}
  def read(file, key) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_ss_table(disk),
           {:ok, {_, _, _, range_size, range_pos, no_of_blocks, _}} <- read_metadata(disk),
           {:ok, range} <- read_range(disk, range_pos, range_size),
           :ok <- key_in_range(range, key) do
        binary_search(disk, key, 0, no_of_blocks)
      end

    Disk.close(disk)
    result
  end

  defp binary_search(_disk, _key, low, high) when high < low, do: :error

  defp binary_search(disk, key, low, high) do
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

  defp read_block(disk, position) do
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

  @doc """
  Reads and returns the bloom filter from an SSTable file.

  Extracts the bloom filter and level information from the SSTable's metadata
  section. The bloom filter is used for efficient key existence checks before
  performing expensive disk searches.

  ## Parameters

  - `file` - SSTable file name

  ## Returns

  `{:ok, bloom_filter, level}` on success, or `{:error, reason}` on failure.
  """
  @spec fetch_ss_table_info(SeaGoat.db_file()) ::
          {:ok, {SeaGoat.BloomFilter.t(), non_neg_integer()}} | {:error, term()}
  def fetch_ss_table_info(file) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_ss_table(disk),
           {:ok, {level, bf_size, bf_pos, _, _, _, _}} <- read_metadata(disk),
           {:ok, bf} <- read_bloom_filter(disk, bf_pos, bf_size) do
        {:ok, bf, level}
      end

    Disk.close(disk)
    result
  end

  defp valid_ss_table(disk) do
    with {:ok, magic} <- Disk.read_from_end(disk, SSTable.size(:magic), SSTable.size(:magic)),
         true <- SSTable.is_ss_table(magic) do
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
             SSTable.size(:magic) + SSTable.size(:metadata),
             SSTable.size(:metadata)
           ) do
      SSTable.decode_metadata(encoded_metadata)
    end
  end

  defp read_bloom_filter(disk, pos, size) do
    with {:ok, encoded_bf} <- Disk.read(disk, pos, size) do
      SSTable.decode_bloom_filter(encoded_bf)
    end
  end

  defp read_range(disk, pos, size) do
    with {:ok, encoded_range} <- Disk.read(disk, pos, size) do
      SSTable.decode_range(encoded_range)
    end
  end

  defp key_in_range({smallest, largest}, key) when key >= smallest and key <= largest, do: :ok
  defp key_in_range(_, _), do: :error
end
