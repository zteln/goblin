defmodule SeaGoat.SSTables do
  @moduledoc """
  Provides operations for managing SSTable files on disk.

  SSTables (Sorted String Tables) are immutable, on-disk data structures used for 
  efficient storage and retrieval of key-value pairs. This module handles the 
  complete lifecycle of SSTable files including creation, reading, file management,
  and key lookups.
  """
  alias SeaGoat.BloomFilter
  alias SeaGoat.SSTables.Util
  alias SeaGoat.SSTables.Disk
  alias SeaGoat.SSTables.Iterator

  def delete(file), do: Disk.rm(file)

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
  def switch(from, to), do: Disk.rename(from, to)

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
  @spec write(Iterator.t(), SeaGoat.db_file(), non_neg_integer()) ::
          {:ok, BloomFilter.t(), SeaGoat.db_file(), non_neg_integer()}
  def write(iterator, file, level) do
    with {:ok, bloom_filter} <- Util.write_to_file(file, level, iterator) do
      {:ok, bloom_filter}
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
      with :ok <- Util.valid_ss_table(disk),
           {:ok, {_, _, _, range_size, range_pos, _, _, _, _, no_of_blocks, _}} <-
             Util.read_metadata(disk),
           {:ok, range} <- Util.read_range(disk, range_pos, range_size),
           :ok <- Util.key_in_range(range, key) do
        Util.binary_search(disk, key, 0, no_of_blocks)
      end

    Disk.close(disk)
    result
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
      with :ok <- Util.valid_ss_table(disk),
           {:ok,
            {level, bf_size, bf_pos, _, _, min_seq_size, min_seq_pos, max_seq_size, max_seq_pos,
             _, _}} <- Util.read_metadata(disk),
           {:ok, bf} <- Util.read_bloom_filter(disk, bf_pos, bf_size),
           {:ok, min_seq} <- Util.read_sequence(disk, min_seq_pos, min_seq_size),
           {:ok, max_seq} <- Util.read_sequence(disk, max_seq_pos, max_seq_size) do
        {:ok, bf, level, min_seq, max_seq}
      end

    Disk.close(disk)
    result
  end
end
