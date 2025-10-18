defmodule SeaGoat.SSTables do
  @moduledoc false
  alias SeaGoat.SSTables.Util
  alias SeaGoat.SSTables.Disk

  def delete(file), do: Disk.rm(file)

  @spec switch(SeaGoat.db_file(), SeaGoat.db_file()) :: :ok
  def switch(from, to), do: Disk.rename(from, to)

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
