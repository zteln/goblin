defmodule SeaGoatDB.Actions do
  @moduledoc false
  alias SeaGoatDB.WAL
  alias SeaGoatDB.Manifest
  alias SeaGoatDB.SSTs
  alias SeaGoatDB.Store
  alias SeaGoatDB.RWLocks

  @flush_level 0

  @spec flush(
          [{SeaGoatDB.db_sequence(), SeaGoatDB.db_key(), SeaGoatDB.db_value()}],
          WAL.rotated_file(),
          SeaGoatDB.key_limit(),
          {Store.store(), WAL.wal(), Manifest.manifest()}
        ) :: {:ok, :flushed} | {:error, term()}
  def flush(data, rotated_wal, key_limit, {store, wal, manifest}) do
    stream = flush_stream(data)

    with {:ok, flushed} <- write_flush(stream, key_limit, store),
         {:ok, flushed} <- switch(flushed),
         :ok <- log_flush(flushed, rotated_wal, manifest),
         :ok <- WAL.clean(wal, rotated_wal),
         :ok <- put_in_store(flushed, store) do
      {:ok, :flushed}
    end
  end

  defp write_flush(stream, key_limit, store) do
    file = Store.new_file(store)
    tmp_file = Store.tmp_file(file)

    stream
    |> Stream.chunk_every(key_limit)
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, flushed} ->
      case SSTs.write(tmp_file, @flush_level, chunk) do
        {:ok, bloom_filter, priority, size, key_range} ->
          {:cont, {:ok, [{tmp_file, file, {bloom_filter, priority, size, key_range}} | flushed]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp switch(flushed, acc \\ [])
  defp switch([], acc), do: {:ok, acc}

  defp switch([{tmp_file, file, sst_info} | flushed], acc) do
    case SSTs.switch(tmp_file, file) do
      :ok -> switch(flushed, [{file, sst_info} | acc])
      {:error, _reason} = error -> error
    end
  end

  defp log_flush(flushed, rotated_wal, manifest) do
    files = Enum.map(flushed, &elem(&1, 0))
    Manifest.log_flush(manifest, files, rotated_wal)
  end

  defp put_in_store([], _store), do: :ok

  defp put_in_store([{file, sst_info} | flushed], store) do
    case Store.put(store, file, @flush_level, sst_info) do
      :ok -> put_in_store(flushed, store)
      {:error, _reason} = error -> error
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
          [SeaGoatDB.db_file()],
          SeaGoatDB.db_level_key(),
          SeaGoatDB.Compactor.Level.t(),
          (SeaGoatDB.Compactor.Level.t(),
           {SeaGoatDB.db_sequence(), SeaGoatDB.db_key(), SeaGoatDB.db_value()} ->
             SeaGoatDB.Compactor.Level.t()),
          boolean(),
          SeaGoatDB.db_key_limit(),
          {Store.store(), Manifest.manifest(), RWLocks.rw_locks()}
        ) :: {:ok, [SeaGoatDB.db_file()], SeaGoatDB.db_level_key(), SeaGoatDB.db_level_key()}
  def merge(
        sources,
        source_level_key,
        target_level,
        level_reducer,
        clean_tombstones?,
        key_limit,
        {store, manifest, rw_locks}
      ) do
    target_level = deplete(sources, target_level, level_reducer)

    with {:ok, old, new} <-
           merge_reduce(
             Enum.to_list(target_level.entries),
             target_level.level_key,
             clean_tombstones?,
             key_limit,
             store
           ),
         :ok <- Manifest.log_compaction(manifest, sources ++ old, Enum.map(new, &elem(&1, 0))),
         :ok <- put_new_files_in_store(new, target_level.level_key, store),
         :ok <- remove_old_files(sources ++ old, store, rw_locks) do
      {:ok, sources ++ old, source_level_key, target_level.level_key}
    end
  end

  defp deplete([], level, _level_reducer), do: level

  defp deplete([source | sources], level, level_reducer) do
    level =
      source
      |> SSTs.stream!()
      |> Enum.reduce(level, level_reducer)

    deplete(sources, level, level_reducer)
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
    with {:ok, bloom_filter, seq, size, key_range} <- SSTs.write(tmp_file, level_key, chunk),
         :ok <- SSTs.switch(tmp_file, file) do
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
         :ok <- SSTs.delete(file),
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
  defp init_sst_iter(id), do: SSTs.iterate(id)

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
    case SSTs.iterate(iter) do
      :ok -> {:halt, :ok}
      {:ok, data, iter} -> {[data], {iter, [], nil}}
    end
  end

  defp iter_merge_data({iter, [], placeholder}), do: {[placeholder], {iter, [], nil}}

  defp iter_merge_data({iter, [next | buffer], nil}) do
    case SSTs.iterate(iter) do
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
