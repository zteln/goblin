defmodule SeaGoatDB.SSTs do
  @moduledoc false
  alias SeaGoatDB.SSTs.Disk
  alias SeaGoatDB.SSTs.SST
  alias SeaGoatDB.BloomFilter

  @tmp_suffix ".tmp"

  def delete(file), do: Disk.rm(file)

  def find(file, key) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_ss_table(disk),
           {:ok, {_, _, _, key_range_pos, key_range_size, _, _, no_of_blocks, _, _}} <-
             read_metadata(disk),
           {:ok, key_range} <- read_key_range(disk, key_range_pos, key_range_size),
           :ok <- key_in_range(key_range, key) do
        binary_search(disk, key, 0, no_of_blocks)
      end

    Disk.close(disk)
    result
  end

  @spec fetch_sst_info(SeaGoatDB.db_file()) ::
          {:ok, {SeaGoatDB.BloomFilter.t(), non_neg_integer()}} | {:error, term()}
  def fetch_sst_info(file) do
    disk = Disk.open!(file)

    result =
      with :ok <- valid_ss_table(disk),
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
            }} <- read_metadata(disk),
           {:ok, bf} <- read_bloom_filter(disk, bf_pos, bf_size),
           {:ok, key_range} <- read_key_range(disk, key_range_pos, key_range_size),
           {:ok, priority} <- read_priority(disk, priority_pos, priority_size) do
        {:ok, bf, level_key, priority, size, key_range}
      end

    Disk.close(disk)
    result
  end

  def stream!(file) do
    Stream.resource(
      fn ->
        Disk.open!(file, start?: true)
      end,
      fn disk ->
        case read_next_key(disk) do
          {:ok, data, disk} ->
            {[data], disk}

          {:error, :eod} ->
            {:halt, disk}

          {:error, _reason} ->
            Disk.close(disk)
            raise "stream failed"
        end
      end,
      fn disk -> Disk.close(disk) end
    )
  end

  def iterate({:next, disk}) do
    case read_next_key(disk) do
      {:ok, data, disk} ->
        {:ok, data, {:next, disk}}

      {:error, :eod} ->
        Disk.close(disk)
        :ok

      e ->
        Disk.close(disk)
        e
    end
  end

  def iterate(file) do
    disk = Disk.open!(file, start?: true)
    {:next, disk}
  end

  def flush(data, level_key, key_limit, file_getter) do
    stream = flush_stream(data, key_limit)

    with {:ok, flushed} <- write_stream(stream, level_key, file_getter) do
      switch(flushed)
    end
  end

  def merge(target, key_limit, clean_tombstones?, file_getter) do
    %{
      level_key: level_key,
      entries: entries
    } = target

    old = Map.keys(entries)

    streams =
      Enum.map(entries, fn {_id, entry} ->
        merge_stream(entry, clean_tombstones?, key_limit)
      end)

    with {:ok, merged} <- write_streams(streams, level_key, file_getter),
         {:ok, merged} <- switch(merged) do
      {:ok, old, merged}
    end
  end

  defp write_streams(streams, level_key, file_getter, acc \\ [])
  defp write_streams([], _level_key, _file_getter, acc), do: {:ok, acc}

  defp write_streams([stream | rest], level_key, file_getter, acc) do
    with {:ok, new} <- write_stream(stream, level_key, file_getter) do
      write_streams(rest, level_key, file_getter, new ++ acc)
    end
  end

  defp write_stream(stream, level_key, file_getter) do
    Enum.reduce_while(stream, {:ok, []}, fn chunk, {:ok, acc} ->
      file = file_getter.()
      tmp_file = tmp_file(file)

      case write(tmp_file, level_key, chunk) do
        {:ok, bloom_filter, priority, size, key_range} ->
          {:cont, {:ok, [{tmp_file, file, {bloom_filter, priority, size, key_range}} | acc]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp flush_stream(data, key_limit) do
    Stream.resource(
      fn -> data end,
      &iter_flush_data/1,
      &after_iter/1
    )
    |> Stream.chunk_every(key_limit)
  end

  defp iter_flush_data([]), do: {:halt, :ok}
  defp iter_flush_data([next | data]), do: {[next], data}

  defp merge_stream(entry, clean_tombstones?, key_limit) do
    Stream.resource(
      fn ->
        sst_iter = init_sst_iter(entry.id)
        buffer = init_buffer(entry.buffer, clean_tombstones?)
        {sst_iter, buffer, nil}
      end,
      &iter_merge_data/1,
      &after_iter/1
    )
    |> Stream.chunk_every(key_limit)
  end

  defp init_sst_iter(nil), do: nil
  defp init_sst_iter(id), do: iterate(id)

  defp init_buffer(buffer, true) do
    buffer
    |> Enum.reject(fn {_key, {_seq, value}} -> value == :tombstone end)
    |> init_buffer(false)
  end

  defp init_buffer(buffer, false) do
    buffer
    |> Enum.map(fn {key, {seq, value}} -> {seq, key, value} end)
    |> Enum.sort_by(fn {_seq, key, _value} -> key end, :asc)
  end

  defp iter_merge_data({nil, [], nil}), do: {:halt, :ok}
  defp iter_merge_data({nil, [next | buffer], nil}), do: {[next], {nil, buffer, nil}}

  defp iter_merge_data({iter, [], nil}) do
    case iterate(iter) do
      :ok -> {:halt, :ok}
      {:ok, data, iter} -> {[data], {iter, [], nil}}
    end
  end

  defp iter_merge_data({iter, [], placeholder}), do: {[placeholder], {iter, [], nil}}

  defp iter_merge_data({iter, [next | buffer], nil}) do
    case iterate(iter) do
      :ok -> {[next], {nil, buffer, nil}}
      {:ok, data, iter} -> iter_merge_data({iter, [next | buffer], data})
    end
  end

  defp iter_merge_data({iter, [next | buffer], placeholder}) do
    {next, back, placeholder} = choose_next(next, placeholder)
    buffer = List.wrap(back) ++ buffer
    {[next], {iter, buffer, placeholder}}
  end

  defp choose_next({seq1, key1, _} = data1, {seq2, key2, _} = data2) do
    cond do
      key1 == key2 and seq1 > seq2 -> {data1, nil, nil}
      key1 == key2 and seq1 < seq2 -> {data2, nil, nil}
      key1 < key2 -> {data1, nil, data2}
      key1 > key2 -> {data2, data1, nil}
    end
  end

  defp write(file, level_key, data) do
    disk = Disk.open!(file, write?: true)

    result =
      with {:ok, _disk, bloom_filter, priority, size, key_range} <-
             write_sst(disk, level_key, data) do
        {:ok, bloom_filter, priority, size, key_range}
      end

    Disk.sync(disk)
    Disk.close(disk)
    result
  end

  defp valid_ss_table(disk) do
    with {:ok, magic} <- Disk.read_from_end(disk, SST.size(:magic), SST.size(:magic)),
         true <- SST.is_ss_table(magic) do
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
             SST.size(:magic) + SST.size(:metadata),
             SST.size(:metadata)
           ) do
      SST.decode_metadata(encoded_metadata)
    end
  end

  defp read_bloom_filter(disk, pos, size) do
    with {:ok, encoded_bf} <- Disk.read(disk, pos, size) do
      SST.decode_bloom_filter(encoded_bf)
    end
  end

  defp read_key_range(disk, pos, size) do
    with {:ok, encoded_key_range} <- Disk.read(disk, pos, size) do
      SST.decode_key_range(encoded_key_range)
    end
  end

  defp read_priority(disk, pos, size) do
    with {:ok, encoded_priority} <- Disk.read(disk, pos, size) do
      SST.decode_priority(encoded_priority)
    end
  end

  defp binary_search(_disk, _key, low, high) when high < low, do: :error

  defp binary_search(disk, key, low, high) do
    mid = div(low + high, 2)
    position = (mid - 1) * SST.size(:block)

    with {:ok, seq, k, v} <- read_block(disk, position) do
      cond do
        key == k ->
          v = if v == :tombstone, do: nil, else: v
          {:ok, {:value, seq, v}}

        key < k ->
          binary_search(disk, key, low, mid - 1)

        key > k ->
          binary_search(disk, key, mid + 1, high)
      end
    end
  end

  defp read_block(disk, position) do
    position = max(0, position)

    with {:ok, encoded_header} <- Disk.read(disk, position, SST.size(:block_header)),
         {:ok, span} <- SST.block_span(encoded_header),
         {:ok, encoded} <- Disk.read(disk, position, SST.size(:block) * span),
         {:ok, {seq, key, value}} <- SST.decode_block(encoded) do
      {:ok, seq, key, value}
    else
      {:error, :not_block_start} ->
        read_block(disk, position - SST.size(:block))

      e ->
        e
    end
  end

  defp read_next_key(disk) do
    with {:ok, enc_header} <- Disk.read(disk, SST.size(:block_header)),
         {:ok, span} <- SST.block_span(enc_header),
         {:ok, enc_block} <- Disk.read(disk, SST.size(:block) * span),
         {:ok, data} <- SST.decode_block(enc_block) do
      {:ok, data, Disk.advance_offset(disk, SST.size(:block) * span)}
    end
  end

  defp write_sst(disk, level_key, data) do
    with {:ok, disk, data} <- write_data(disk, data),
         {:ok, disk, bloom_filter, meta_size} <- write_meta(disk, level_key, data) do
      {:ok, disk, bloom_filter, data.priority, data.size + meta_size, data.key_range}
    end
  end

  defp write_data(disk, data) do
    init = %{
      priority: nil,
      no_of_keys: 0,
      no_of_blocks: 0,
      key_range: {nil, nil},
      bloom_filter: BloomFilter.new(),
      size: 0
    }

    Enum.reduce_while(data, {:ok, disk, init}, fn {seq, k, v}, {:ok, disk, acc} ->
      block = SST.encode_block(seq, k, v)
      block_size = byte_size(block)
      span = SST.span(block_size)
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

  defp write_meta(disk, level_key, data) do
    bloom_filter = BloomFilter.generate(data.bloom_filter)

    footer =
      SST.encode_footer(
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

  defp key_in_range({smallest, largest}, key) when key >= smallest and key <= largest, do: :ok
  defp key_in_range(_, _), do: :error

  defp switch(to_switch, acc \\ [])
  defp switch([], acc), do: {:ok, acc}

  defp switch([{from, to, write_data} | to_switch], acc) do
    with :ok <- Disk.rename(from, to) do
      switch(to_switch, [{to, write_data} | acc])
    end
  end

  defp after_iter(_), do: :ok
  defp tmp_file(file), do: file <> @tmp_suffix
end
