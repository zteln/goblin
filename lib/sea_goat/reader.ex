defmodule SeaGoat.Reader do
  alias SeaGoat.LockManager
  alias SeaGoat.FileHandler
  alias SeaGoat.Block

  @file_suffix ".seagoat"

  def fetch_latest_block_count(dir) do
    with {:ok, files} <- File.ls(dir) do
      latest_block_count =
        files
        |> Enum.filter(&String.ends_with?(&1, @file_suffix))
        |> latest_block_count()

      {:ok, latest_block_count + 1}
    end
  end

  defp latest_block_count(files, acc \\ 0)
  defp latest_block_count([], acc), do: acc

  defp latest_block_count([file | files], acc) do
    [count, _ext] = String.split(file, ".", trim: true)

    case Integer.parse(count) do
      {int, _} when int > acc ->
        latest_block_count(files, int)

      _ ->
        latest_block_count(files, acc)
    end
  end

  def fetch_levels(dir, lock_manager) do
    with {:ok, files} <- File.ls(dir) do
      files
      |> sort_block_files(dir)
      |> get_levels(lock_manager)
    end
  end

  defp sort_block_files(files, dir) do
    files
    |> Enum.filter(&String.ends_with?(&1, @file_suffix))
    |> Enum.map(fn file ->
      [block_no, _ext] = String.split(file, ".", trim: true)
      {String.to_integer(block_no), file}
    end)
    |> List.keysort(0, :desc)
    |> Enum.map(&Path.join(dir, elem(&1, 1)))
  end

  defp get_levels(paths, lock_manager, acc \\ [])
  defp get_levels([], _lock_manager, acc), do: {:ok, acc}

  defp get_levels([path | paths], lock_manager, acc) do
    case get_level_and_bloom_filter(path, lock_manager) do
      {:ok, level, bloom_filter} ->
        get_levels(paths, lock_manager, [{path, bloom_filter, level} | acc])

      {:error, _reason} = e ->
        e
    end
  end

  defp get_level_and_bloom_filter(path, lock_manager) do
    with :ok <- LockManager.acquire_shared_lock(lock_manager, path),
         {:ok, io, offset} <- FileHandler.open(path),
         {:ok, encoded_footer} <-
           FileHandler.read(io, Block.offset_calc(offset, :footer), Block.metafooter_size()),
         {:ok, {level, _range_size, _index_size, bloom_filter_size} = footer} <-
           Block.decode_metafooter(encoded_footer),
         {:ok, encoded_bloom_filter} <-
           FileHandler.read(
             io,
             Block.offset_calc(offset, footer, :bloom_filter),
             bloom_filter_size
           ),
         {:ok, bloom_filter} <- Block.decode_metadata(encoded_bloom_filter),
         :ok <- FileHandler.close(io),
         :ok <- LockManager.release_lock(lock_manager, path) do
      {:ok, level, bloom_filter}
    end
  end

  def read_files([], _key, _lock_manager), do: {:ok, nil}

  def read_files([file | files], key, lock_manager) do
    :ok = LockManager.acquire_shared_lock(lock_manager, file)

    case read_in_file(file, key) do
      {:ok, {:value, value}} ->
        :ok = LockManager.release_lock(lock_manager, file)
        {:ok, value}

      {:error, _reason} = e ->
        :ok = LockManager.release_lock(lock_manager, file)
        e

      _ ->
        :ok = LockManager.release_lock(lock_manager, file)
        read_files(files, key, lock_manager)
    end
  end

  defp read_in_file(file, key) do
    with {:ok, io, offset} <- FileHandler.open(file),
         {:ok, {index, size}} <- read_in_meta(io, offset, key) do
      result = read_data(io, key, index, size)
      FileHandler.close(io)
      result
    end
  end

  defp read_in_meta(io, offset, key) do
    with {:ok, encoded_footer} <-
           FileHandler.read(io, Block.offset_calc(offset, :footer), Block.metafooter_size()),
         {:ok, {_level, range_size, index_size, _bloom_filter_size} = footer} <-
           Block.decode_metafooter(encoded_footer),
         {:ok, encoded_range} <-
           FileHandler.read(io, Block.offset_calc(offset, footer, :range), range_size),
         {:ok, {smallest, largest}} when key >= smallest and key <= largest <-
           Block.decode_metadata(encoded_range),
         {:ok, encoded_index} <-
           FileHandler.read(io, Block.offset_calc(offset, footer, :index), index_size),
         {:ok, index} <- Block.decode_metadata(encoded_index) do
      {:ok, Map.get(index, key)}
    else
      {:ok, _} -> {:ok, :next}
      {:error, _reason} = e -> e
    end
  end

  defp read_data(io, key, offset, size) do
    with {:ok, block} <- FileHandler.read(io, offset, size),
         {:ok, {^key, value}} <- Block.decode_block(block) do
      value = if value == :tombstone, do: nil, else: value
      {:ok, {:value, value}}
    end
  end
end
