defmodule SeaGoat.Blocks do
  alias SeaGoat.FileHandler
  alias SeaGoat.BloomFilter
  alias __MODULE__.Block

  @task_timeout :timer.minutes(5)

  def delete_block(path) do
    :ok = FileHandler.rm(path)
  end

  def switch(from, to) do
    :ok = FileHandler.rename(from, to)
  end

  def flush(mem_table, pass_path, path, level) do
    with {:ok, bloom_filter} <-
           write_to_file(path, Enum.sort(mem_table), level, &next_flush_data/1) do
      {:flushed, bloom_filter, pass_path, path}
    end
  end

  def merge(blocks_to_merge, pass_path, path, level) do
    with {:ok, open_blocks} <- open_blocks(blocks_to_merge),
         {:ok, bloom_filter} <-
           write_to_file(path, open_blocks, level, &next_merge_data/1),
         :ok <- close_blocks(open_blocks) do
      {:merged, blocks_to_merge, pass_path, path, bloom_filter, level}
    end
  end

  def read_bloom_filter(file) do
    read_f = fn io, offset, footer ->
      tier = Block.footer_part(footer, :level)

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

  def read_key(files, key) do
    files
    |> Task.async_stream(&search_for_key(&1, key), timeout: @task_timeout)
    |> Stream.map(fn {:ok, res} -> res end)
    |> Stream.filter(& &1)
    |> Stream.take(1)
    |> Enum.to_list()
  end

  defp write_to_file(path, data, level, next) do
    starting_offset = 0

    with {:ok, io, ^starting_offset} <- FileHandler.open(path, write?: true),
         {:ok, bloom_filter} <- write(io, starting_offset, data, level, next),
         :ok <- FileHandler.sync(io),
         :ok <- FileHandler.close(io) do
      {:ok, bloom_filter}
    end
  end

  defp write(io, offset, data, level, next) do
    with {:ok, offset} <- FileHandler.write(io, offset, Block.make()),
         {:ok, index, offset} <- write_data(io, offset, data, next) do
      write_meta(io, offset, index, level)
    end
  end

  defp write_data(io, offset, data, next, index \\ %{}) do
    case next.(data) do
      {:next, {k, v}, data} ->
        block = Block.encode(:data, key: k, value: v)
        {:ok, new_offset} = FileHandler.write(io, offset, block)
        index = Map.put(index, k, {offset, byte_size(block)})
        write_data(io, new_offset, data, next, index)

      :eod ->
        {:ok, index, offset}
    end
  end

  defp write_meta(io, offset, index, level) do
    keys = Map.keys(index)
    range = {Enum.min(keys), Enum.max(index)}
    bloom_filter = BloomFilter.new(keys, length(keys))

    meta =
      Block.encode(:meta, level: level, range: range, index: index, bloom_filter: bloom_filter)

    with {:ok, _offset} <- FileHandler.write(io, offset, meta) do
      {:ok, bloom_filter}
    end
  end

  defp open_blocks(blocks, acc \\ [])
  defp open_blocks([], acc), do: {:ok, acc}

  defp open_blocks([block | blocks], acc) do
    starting_offset = 0

    with {:ok, io, ^starting_offset} <- FileHandler.open(block, start?: true),
         {:ok, offset, kv} <- next_kv(io, starting_offset + Block.size(:header)) do
      open_blocks(blocks, [{io, offset, kv, block} | acc])
    end
  end

  defp close_blocks([]), do: :ok

  defp close_blocks([{io, _, _, _} | blocks]) do
    with :ok <- FileHandler.close(io) do
      close_blocks(blocks)
    end
  end

  defp next_flush_data([]), do: :eod

  defp next_flush_data([next | data]) do
    {:next, next, data}
  end

  defp next_merge_data([]), do: :eod

  defp next_merge_data(open_blocks) do
    {k, v} =
      open_blocks
      |> Enum.map(&elem(&1, 2))
      |> take_smallest_kv()

    {:ok, open_blocks} = advance(open_blocks, k)
    {:next, {k, v}, open_blocks}
  end

  defp take_smallest_kv(kvs) do
    kvs
    |> Enum.reduce(hd(kvs), fn {k, v}, {smallest_key, _} = smallest_kv ->
      if k < smallest_key do
        {k, v}
      else
        smallest_kv
      end
    end)
  end

  defp advance(open_blocks, key, acc \\ [])
  defp advance([], _key, acc), do: {:ok, Enum.reverse(acc)}

  defp advance([{io, offset, {key, _v}, block} | open_blocks], key, acc) do
    with {:ok, offset, kv} <- next_kv(io, offset) do
      acc = if kv, do: [{io, offset, kv, block} | acc], else: acc
      advance(open_blocks, key, acc)
    end
  end

  defp advance([open_block | open_blocks], key, acc) do
    advance(open_blocks, key, [open_block | acc])
  end

  defp next_kv(io, offset) do
    with {:ok, encoded_kv_size} <- FileHandler.read(io, offset, Block.size(:kv_header)),
         {:ok, kv_size} <- get_size(offset, encoded_kv_size),
         {:ok, encoded_data} <- FileHandler.read(io, offset, Block.size(:kv_header) + kv_size),
         {:ok, kv} <- Block.decode(:data, encoded_data) do
      {:ok, offset + Block.size(:kv_header) + kv_size, kv}
    end
  end

  defp get_size(offset, encoded_kv_size) do
    case Block.decode(:kv_header, encoded_kv_size) do
      {:ok, :end_of_data} ->
        {:ok, offset, nil}

      {:ok, kv_size} ->
        {:ok, kv_size}

      e ->
        e
    end
  end

  defp search_for_key(file, key) do
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

  defp read_file(file, f) do
    {io, offset} = FileHandler.open!(file)

    case f.(io, offset) do
      {:ok, data} ->
        FileHandler.close(io)
        {:ok, data}

      e ->
        FileHandler.close(io)
        e
    end
  end

  defp read_meta(io, offset, f) do
    with {:ok, encoded_footer} <-
           FileHandler.read(io, Block.offset_calc(offset, :footer), Block.size(:footer)),
         {:ok, footer} <- Block.decode(:footer, encoded_footer) do
      f.(io, offset, footer)
    end
  end

  defp read_meta_part(io, offset, footer, meta_part_key) do
    with {:ok, encoded} <-
           FileHandler.read(
             io,
             Block.offset_calc(offset, footer, meta_part_key),
             Block.footer_part(footer, meta_part_key)
           ),
         {:ok, decoded} <- Block.decode(:meta_part, encoded) do
      {:ok, decoded}
    end
  end

  defp read_data(io, key_offset, size, key) do
    with {:ok, encoded} <- FileHandler.read(io, key_offset, size),
         {:ok, {fetched_key, value}} <- Block.decode(:data, encoded) do
      if fetched_key == key do
        value = if value == :tombstone, do: nil, else: value
        {:ok, {:value, value}}
      else
        {:error, :next}
      end
    end
  end
end
