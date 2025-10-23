defmodule SeaGoatDB.SSTs do
  @moduledoc false
  alias SeaGoatDB.SSTs.Util
  alias SeaGoatDB.SSTs.Disk

  @tmp_suffix ".tmp"

  def flush(data, level_key, key_limit, file_getter) do
    with {:ok, flushed} <- write_flush(data, level_key, key_limit, file_getter) do
      switch(flushed)
    end
  end

  defp write_flush(data, level_key, key_limit, file_getter) do
    data
    |> flush_stream()
    |> Stream.chunk_every(key_limit)
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, flushed} ->
      file = file_getter.()
      tmp_file = tmp_file(file)

      case write(tmp_file, level_key, chunk) do
        {:ok, bloom_filter, priority, size, key_range} ->
          {:cont, {:ok, [{tmp_file, file, {bloom_filter, priority, size, key_range}} | flushed]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp flush_stream(data) do
    Stream.resource(
      fn -> data end,
      &iter_flush_data/1,
      &after_iter/1
    )
  end

  defp iter_flush_data([]), do: {:halt, :ok}
  defp iter_flush_data([next | data]), do: {[next], data}

  def merge(sources, target, key_limit, clean_tombstones?, level_reducer, file_getter) do
    target = deplete(sources, target, level_reducer)

    with {:ok, old, new} <-
           do_merge(
             Enum.to_list(target.entries),
             target.level_key,
             clean_tombstones?,
             key_limit,
             file_getter
           ),
         {:ok, new} <- switch(new) do
      {:ok, old, new}
    end
  end

  defp deplete([], level, _level_reducer), do: level

  defp deplete([source | sources], level, level_reducer) do
    level =
      source
      |> stream!()
      |> Enum.reduce(level, level_reducer)

    deplete(sources, level, level_reducer)
  end

  defp do_merge(entries, level_key, clean_tombstones?, key_limit, file_getter, acc \\ {[], []})

  defp do_merge([], _level_key, _clean_tombstones?, _key_limit, _file_getter, {old, new}),
    do: {:ok, old, new}

  defp do_merge(
         [{_id, entry} | entries],
         level_key,
         clean_tombstones?,
         key_limit,
         file_getter,
         {acc_old, acc_new}
       ) do
    with {:ok, old, new} <-
           write_compaction(entry, level_key, clean_tombstones?, key_limit, file_getter) do
      do_merge(
        entries,
        level_key,
        clean_tombstones?,
        key_limit,
        file_getter,
        {old ++ acc_old, new ++ acc_new}
      )
    end
  end

  defp write_compaction(%{buffer: []}, _level_key, _clean_tombstones?, _key_limit, _file_getter),
    do: {:ok, [], []}

  defp write_compaction(entry, level_key, clean_tombstones?, key_limit, file_getter) do
    entry
    |> merge_stream(clean_tombstones?)
    |> Stream.chunk_every(key_limit)
    |> Enum.reduce_while({:ok, List.wrap(entry.id), []}, fn chunk, {:ok, old, new} ->
      file = file_getter.()
      tmp_file = tmp_file(file)

      case write(tmp_file, level_key, chunk) do
        {:ok, bloom_filter, seq, size, key_range} ->
          {:cont, {:ok, old, [{tmp_file, file, {bloom_filter, seq, size, key_range}} | new]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp merge_stream(entry, clean_tombstones?) do
    Stream.resource(
      fn ->
        sst_iter = init_sst_iter(entry.id)
        buffer = init_buffer(entry.buffer, clean_tombstones?)
        {sst_iter, buffer, nil}
      end,
      &iter_merge_data/1,
      &after_iter/1
    )
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

  def stream!(file) do
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
        Disk.close(disk)
        e
    end
  end

  def iterate(file) do
    disk = Disk.open!(file, start?: true)
    {:next, disk}
  end

  @spec fetch_ss_table_info(SeaGoatDB.db_file()) ::
          {:ok, {SeaGoatDB.BloomFilter.t(), non_neg_integer()}} | {:error, term()}
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

  def delete(file), do: Disk.rm(file)

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
