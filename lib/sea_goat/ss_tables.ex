defmodule SeaGoat.SSTables do
  @moduledoc """
  Provides operations for managing SSTable files on disk.

  SSTables (Sorted String Tables) are immutable, on-disk data structures used for 
  efficient storage and retrieval of key-value pairs. This module handles the 
  complete lifecycle of SSTable files including creation, reading, file management,
  and key lookups.
  """
  # alias SeaGoat.BloomFilter
  alias SeaGoat.SSTables.Util
  alias SeaGoat.SSTables.Disk
  # alias SeaGoat.SSTables.Iterator

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

  # @doc """
  # Writes key-value data to an SSTable file on disk.
  #
  # Creates a new SSTable file containing the data provided through the iterator. 
  # The iterator determines how data is sourced and ordered:
  #
  # - `MemTableIterator` - sorts in-memory data before writing
  # - `SSTablesIterator` - merges multiple existing SSTables in sorted order
  #
  # Returns the bloom filter, file, and level upon successful completion.
  #
  # ## Parameters
  #
  # - `iterator` - Iterator struct that implements the Iterator protocol
  # - `data` - Data source passed to the iterator (mem_table, files, etc.)
  # - `file` - File where the SSTable will be created
  # - `level` - SSTable level for LSM tree organization
  #
  # ## Returns
  #
  # `{:ok, bloom_filter, file, level}` on success, or `{:error, reason}` on failure.
  # """
  # @spec write(Iterator.t(), SeaGoat.db_file(), non_neg_integer()) ::
  #         {:ok, BloomFilter.t(), SeaGoat.db_file(), non_neg_integer()}
  # def write(iterator, file, level) do
  #   with {:ok, disk} <- Disk.open(file, write?: true),
  #        {:ok, disk, bloom_filter, smallest_seq, written_size, range} <-
  #          Util.write_ss_table(disk, level, iterator),
  #        :ok <- Disk.sync(disk),
  #        :ok <- Disk.close(disk) do
  #     {:ok, bloom_filter, smallest_seq, written_size, range}
  #   end
  #
  #   # Util.write_to_file(file, level, iterator)
  #   # with {:ok, bloom_filter} <- Util.write_to_file(file, level, iterator) do
  #   #   {:ok, bloom_filter}
  #   # end
  # end
  def write(file, level_key, data) do
    disk = Disk.open!(file, write?: true)

    result =
      with {:ok, _disk, bloom_filter, priority, size, key_range} <-
             Util.write_sst(disk, level_key, data) do
        {:ok, bloom_filter, priority, size, key_range}
      end

    Disk.sync(disk)
    Disk.close(disk)
    result
  end

  def find(file, key) do
    disk = Disk.open!(file)

    result =
      with :ok <- Util.valid_ss_table(disk),
           {:ok, {_, _, _, key_range_pos, key_range_size, _, _, no_of_blocks, _, _}} <-
             Util.read_metadata(disk),
           {:ok, key_range} <- Util.read_key_range(disk, key_range_pos, key_range_size),
           :ok <- Util.key_in_range(key_range, key) do
        Util.binary_search(disk, key, 0, no_of_blocks)
      end

    Disk.close(disk)
    result
  end

  def stream(file) do
    Stream.resource(
      fn ->
        Disk.open!(file, start?: true)
      end,
      fn disk ->
        case Util.read_next_key(disk) do
          {:ok, data, disk} ->
            {[data], disk}

          {:error, :eod} ->
            {:halt, disk}

          {:error, _reason} ->
            raise "stream failed"
        end
      end,
      fn disk -> Disk.close(disk) end
    )
  end

  def iterate({:next, disk}) do
    case Util.read_next_key(disk) do
      {:ok, data, disk} ->
        {:ok, data, {:next, disk}}

      {:error, :eod} ->
        Disk.close(disk)
        :ok

      e ->
        e
    end
  end

  def iterate(file) do
    disk = Disk.open!(file, start?: true)
    {:next, disk}
  end

  # def start_iterate(file) do
  #   disk = Disk.open!(file)
  #
  #   with :ok <- Util.valid_ss_table(disk) do
  #     disk = Disk.start_of_file(disk)
  #     {:ok, {:next, disk}}
  #   end
  # end
  #
  # def iterate({:next, disk}) do
  #   case Util.read_next_key(disk) do
  #     {:ok, data, disk} ->
  #       {:ok, data, {:next, disk}}
  #
  #     {:error, :eod} ->
  #       Disk.close(disk)
  #       :ok
  #
  #     e ->
  #       e
  #   end
  # end
  #
  # def end_iterate({:next, disk}), do: Disk.close(disk)

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
            }} <- Util.read_metadata(disk),
           {:ok, bf} <- Util.read_bloom_filter(disk, bf_pos, bf_size),
           {:ok, key_range} <- Util.read_key_range(disk, key_range_pos, key_range_size),
           {:ok, priority} <- Util.read_priority(disk, priority_pos, priority_size) do
        {:ok, bf, level_key, priority, size, key_range}
      end

    Disk.close(disk)
    result
  end
end
