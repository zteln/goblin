defmodule SeaGoat.Compactor do
  use GenServer
  alias __MODULE__.Entry
  alias __MODULE__.Level
  alias SeaGoat.Store
  alias SeaGoat.SSTables
  alias SeaGoat.RWLocks
  alias SeaGoat.Manifest

  defstruct [
    :store,
    :rw_locks,
    :manifest,
    :key_limit,
    :level_limit,
    levels: %{},
    merging: %{}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:key_limit, :level_limit, :store, :rw_locks, :manifest])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(compactor, level, file, data) do
    GenServer.call(compactor, {:put, level, file, data})
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       store: args[:store],
       rw_locks: args[:rw_locks],
       manifest: args[:manifest],
       key_limit: args[:key_limit],
       level_limit: args[:level_limit]
     }}
  end

  @impl GenServer
  def handle_call({:put, level_key, file, {seq, size, key_range}}, _from, state) do
    entry = %Entry{
      id: file,
      priority: seq,
      size: size,
      key_range: key_range
    }

    level =
      state.levels
      |> Map.get(level_key, %Level{level_key: level_key})
      |> Level.put_entry(entry)

    levels = Map.put(state.levels, level_key, level)
    state = maybe_compact(%{state | levels: levels}, level_key)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({_ref, {:ok, compacted_away_files, level_key, target_level_key}}, state) do
    state =
      Enum.reduce(compacted_away_files, state, fn compacted_away_file, acc ->
        level = process_level_after_compaction(acc.levels, level_key, compacted_away_file)

        target_level =
          process_level_after_compaction(acc.levels, target_level_key, compacted_away_file)

        levels =
          acc.levels
          |> Map.put(level_key, level)
          |> Map.put(target_level_key, target_level)

        %{acc | levels: levels}
      end)
      |> maybe_compact(level_key)
      |> maybe_compact(target_level_key)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    # Handle error, retry compaction
    {:noreply, state}
  end

  defp process_level_after_compaction(levels, level_key, old_file) do
    level = Map.get(levels, level_key)
    level_entries = Map.delete(level.entries, old_file)
    %{level | entries: level_entries, compacting_ref: nil}
  end

  defp maybe_compact(state, level_key) do
    level = Map.get(state.levels, level_key)
    target_level = Map.get(state.levels, level_key + 1, %Level{level_key: level_key + 1})

    cond do
      not (is_nil(target_level.compacting_ref) and is_nil(level.compacting_ref)) ->
        state

      Level.get_total_size(level) >= level_limit(state.level_limit, level_key) ->
        entry = Level.get_highest_prio_entry(level)
        clean_tombstones? = clean_tombstones?(target_level, state.levels)

        ref =
          compact(
            entry.id,
            level,
            target_level,
            clean_tombstones?,
            state.key_limit,
            state.store,
            state.manifest,
            state.rw_locks
          )

        level = %{level | compacting_ref: ref}
        target_level = %{target_level | compacting_ref: ref}

        levels =
          state.levels
          |> Map.put(level_key, level)
          |> Map.put(level_key + 1, target_level)

        %{state | levels: levels}

      true ->
        state
    end
  end

  defp compact(file, level, target_level, clean_tombstones?, key_limit, store, manifest, rw_locks) do
    %{ref: ref} =
      Task.async(fn ->
        target_level = deplete(file, target_level)

        with {:ok, old, new} <- merge(target_level, clean_tombstones?, key_limit, store),
             :ok <- Manifest.log_compaction(manifest, [file | old], Enum.map(new, &elem(&1, 0))),
             :ok <- put_new_files_in_store(new, target_level.level_key, store),
             :ok <- remove_old_files([file | old], store, rw_locks) do
          {:ok, [file | old], level.level_key, target_level.level_key}
        end
      end)

    ref
  end

  defp deplete(file, level) do
    file
    |> SSTables.stream()
    |> Enum.reduce(level, &Level.place_in_buffer(&2, &1))
  end

  defp merge(level, clean_tombstones?, key_limit, store) do
    Enum.reduce_while(
      level.entries,
      {:ok, [], []},
      fn {_id, entry}, {:ok, old, new} ->
        case write_compaction(entry, level.level_key, clean_tombstones?, key_limit, store) do
          {:ok, merged_old, merged_new} ->
            {:cont, {:ok, merged_old ++ old, merged_new ++ new}}

          {:error, _reason} = error ->
            {:halt, error}
        end
      end
    )
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
      fn
        {nil, [], nil} ->
          {:halt, :ok}

        {nil, [next | buffer], nil} ->
          {[next], {nil, buffer, nil}}

        {iter, [], nil} ->
          case SSTables.iterate(iter) do
            :ok ->
              {:halt, :ok}

            {:ok, data, iter} ->
              {[data], {iter, [], nil}}
          end

        {iter, [], placeholder} ->
          {[placeholder], {iter, [], nil}}

        {iter, [next | buffer], nil} ->
          case SSTables.iterate(iter) do
            :ok ->
              {[next], {nil, buffer, nil}}

            {:ok, data, iter} ->
              {next, back, placeholder} = choose_next(next, data)
              buffer = if back, do: [back | buffer], else: buffer
              {[next], {iter, buffer, placeholder}}
          end

        {iter, [next | buffer], placeholder} ->
          {next, back, placeholder} = choose_next(next, placeholder)
          buffer = if back, do: [back | buffer], else: buffer
          {[next], {iter, buffer, placeholder}}
      end,
      fn _ -> :ok end
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

  defp choose_next({seq1, key1, _} = data1, {seq2, key2, _} = data2) do
    cond do
      key1 == key2 and seq1 > seq2 -> {data1, nil, nil}
      key1 == key2 and seq1 < seq2 -> {data2, nil, nil}
      key1 < key2 -> {data1, nil, data2}
      key1 > key2 -> {data2, data1, nil}
    end
  end

  defp level_limit(level_limit, level_key), do: level_limit * :math.pow(10, level_key)

  defp clean_tombstones?(target_level, levels) do
    has_virtual_entry(target_level) && max_level(levels) == target_level.level_key
  end

  defp has_virtual_entry(%{entries: %{nil => %{is_virtual: true}} = entries})
       when map_size(entries) == 1,
       do: true

  defp has_virtual_entry(_), do: false

  defp max_level(levels) do
    levels
    |> Map.keys()
    |> Enum.max()
  end
end
