defmodule SeaGoat.Actions do
  @moduledoc false
  alias SeaGoat.WAL
  alias SeaGoat.Manifest
  alias SeaGoat.SSTables
  alias SeaGoat.Store
  alias SeaGoat.RWLocks

  @flush_level 0

  @spec flush(
          [{SeaGoat.db_sequence(), SeaGoat.db_key(), SeaGoat.db_value()}],
          WAL.rotated_file(),
          {Store.store(), WAL.wal(), Manifest.manifest()}
        ) :: {:ok, :flushed} | {:error, term()}
  def flush(data, rotated_wal, {store, wal, manifest}) do
    file = Store.new_file(store)
    tmp_file = Store.tmp_file(file)
    stream = flush_stream(data)

    with {:ok, bloom_filter, priority, size, key_range} <-
           SSTables.write(tmp_file, @flush_level, stream),
         :ok <- SSTables.switch(tmp_file, file),
         :ok <- Manifest.log_flush(manifest, file, rotated_wal),
         :ok <- WAL.clean(wal, rotated_wal),
         :ok <-
           Store.put(store, file, @flush_level, {bloom_filter, priority, size, key_range}) do
      {:ok, :flushed}
    end
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

  @spec merge(
          SeaGoat.db_file(),
          SeaGoat.db_level_key(),
          SeaGoat.Compactor.Level.t(),
          (SeaGoat.Compactor.Level.t(),
           {SeaGoat.db_sequence(), SeaGoat.db_key(), SeaGoat.db_value()} ->
             SeaGoat.Compactor.Level.t()),
          boolean(),
          SeaGoat.db_key_limit(),
          {Store.store(), Manifest.manifest(), RWLocks.rw_locks()}
        ) :: {:ok, [SeaGoat.db_file()], SeaGoat.db_level_key(), SeaGoat.db_level_key()}
  def merge(
        file,
        level_key,
        target_level,
        reducer,
        clean_tombstones?,
        key_limit,
        {store, manifest, rw_locks}
      ) do
    target_level = deplete(file, target_level, reducer)

    with {:ok, old, new} <-
           merge_reduce(
             Enum.to_list(target_level.entries),
             target_level.level_key,
             clean_tombstones?,
             key_limit,
             store
           ),
         :ok <- Manifest.log_compaction(manifest, [file | old], Enum.map(new, &elem(&1, 0))),
         :ok <- put_new_files_in_store(new, target_level.level_key, store),
         :ok <- remove_old_files([file | old], store, rw_locks) do
      {:ok, [file | old], level_key, target_level.level_key}
    end
  end

  defp deplete(file, level, reducer) do
    file
    |> SSTables.stream()
    |> Enum.reduce(level, reducer)
  end

  defp merge_reduce(entries, level_key, clean_tombstones?, key_limit, store, acc \\ {[], []})

  defp merge_reduce([], _level_key, _clean_tombstones?, _key_limit, _store, {acc_old, acc_new}),
    do: {:ok, acc_old, acc_new}

  defp merge_reduce(
         [{_id, entry} | entries],
         level_key,
         clean_tombstones?,
         key_limit,
         store,
         {acc_old, acc_new}
       ) do
    case write_compaction(entry, level_key, clean_tombstones?, key_limit, store) do
      {:ok, old, new} ->
        merge_reduce(
          entries,
          level_key,
          clean_tombstones?,
          key_limit,
          store,
          {old ++ acc_old, new ++ acc_new}
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp write_compaction(%{buffer: []}, _level_key, _clean_tombstones?, _key_limit, _store),
    do: {:ok, [], []}

  defp write_compaction(entry, level_key, clean_tombstones?, key_limit, store) do
    entry
    |> merge_stream(clean_tombstones?)
    |> Stream.chunk_every(key_limit)
    |> Enum.reduce_while({:ok, List.wrap(entry.id), []}, fn chunk, {:ok, old, new} ->
      file = Store.new_file(store)
      tmp_file = Store.tmp_file(file)

      case write_chunk(chunk, tmp_file, file, level_key) do
        {:ok, bloom_filter, seq, size, key_range} ->
          {:cont, {:ok, old, [{file, {bloom_filter, seq, size, key_range}} | new]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp write_chunk(chunk, tmp_file, file, level_key) do
    with {:ok, bloom_filter, seq, size, key_range} <- SSTables.write(tmp_file, level_key, chunk),
         :ok <- SSTables.switch(tmp_file, file) do
      {:ok, bloom_filter, seq, size, key_range}
    end
  end

  defp put_new_files_in_store([], _level_key, _store), do: :ok

  defp put_new_files_in_store([{file, data} | new], level_key, store) do
    with :ok <- Store.put(store, file, level_key, data) do
      put_new_files_in_store(new, level_key, store)
    end
  end

  defp remove_old_files([], _store, _rw_locks), do: :ok

  defp remove_old_files([file | old], store, rw_locks) do
    with :ok <- Store.remove(store, file),
         :ok <- RWLocks.wlock(rw_locks, file),
         :ok <- SSTables.delete(file),
         :ok <- RWLocks.unlock(rw_locks, file) do
      remove_old_files(old, store, rw_locks)
    end
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
  defp init_sst_iter(id), do: SSTables.iterate(id)

  defp init_buffer(buffer, true) do
    buffer
    |> Enum.filter(fn {_seq, _key, value} -> value != :tombstone end)
    |> init_buffer(false)
  end

  defp init_buffer(buffer, false), do: Enum.reverse(buffer)

  defp iter_merge_data({nil, [], nil}), do: {:halt, :ok}
  defp iter_merge_data({nil, [next | buffer], nil}), do: {[next], {nil, buffer, nil}}

  defp iter_merge_data({iter, [], nil}) do
    case SSTables.iterate(iter) do
      :ok -> {:halt, :ok}
      {:ok, data, iter} -> {[data], {iter, [], nil}}
    end
  end

  defp iter_merge_data({iter, [], placeholder}), do: {[placeholder], {iter, [], nil}}

  defp iter_merge_data({iter, [next | buffer], nil}) do
    case SSTables.iterate(iter) do
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

  defp after_iter(_), do: :ok
end
