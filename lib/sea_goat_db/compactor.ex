defmodule SeaGoatDB.Compactor do
  @moduledoc false
  use GenServer
  require Logger
  alias SeaGoatDB.Compactor.Entry
  alias SeaGoatDB.Compactor.Level
  alias SeaGoatDB.SSTs
  alias SeaGoatDB.Store
  alias SeaGoatDB.Manifest
  alias SeaGoatDB.RWLocks

  defstruct [
    :store,
    :rw_locks,
    :manifest,
    :task_sup,
    :task_mod,
    :key_limit,
    :level_limit,
    levels: %{}
  ]

  @type compactor :: GenServer.server()
  @type data ::
          {SeaGoatDB.db_sequence(), non_neg_integer(), {SeaGoatDB.db_key(), SeaGoatDB.db_key()}}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [
        :key_limit,
        :level_limit,
        :store,
        :rw_locks,
        :manifest,
        :task_sup,
        :task_mod
      ])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(compactor(), SeaGoatDB.db_level_key(), SeaGoatDB.db_file(), data()) :: :ok
  def put(compactor, level_key, file, data) do
    GenServer.call(compactor, {:put, level_key, file, data})
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       key_limit: args[:key_limit],
       level_limit: args[:level_limit],
       rw_locks: args[:rw_locks],
       store: args[:store],
       manifest: args[:manifest],
       task_sup: args[:task_sup],
       task_mod: args[:task_mod] || Task.Supervisor
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
  def handle_info({_ref, {:ok, compacted_away_files, source_level_key, target_level_key}}, state) do
    state =
      Enum.reduce(compacted_away_files, state, fn compacted_away_file, acc ->
        source_level =
          process_level_after_compaction(
            acc.levels,
            source_level_key,
            compacted_away_file
          )

        target_level =
          process_level_after_compaction(
            acc.levels,
            target_level_key,
            compacted_away_file
          )

        levels =
          acc.levels
          |> Map.put(source_level_key, source_level)
          |> Map.put(target_level_key, target_level)

        %{acc | levels: levels}
      end)
      |> maybe_compact(source_level_key)
      |> maybe_compact(target_level_key)

    {:noreply, state}
  end

  def handle_info({ref, {:error, reason}}, state) do
    [{_, %{compacting_ref: {_, retry}} = source_level}, {_, target_level}] =
      state.levels
      |> Enum.filter(fn
        {_level_key, %{compacting_ref: {^ref, _retry}}} -> true
        _ -> false
      end)
      |> List.keysort(0)

    case retry do
      0 ->
        Logger.error(fn ->
          "Failed to compact after 5 attempts with reason: #{inspect(reason)}. Exiting."
        end)

        {:stop, {:error, :failed_to_compact}, state}

      retry ->
        Logger.warning(fn ->
          "Failed to compact with reason: #{inspect(reason)}. Retrying..."
        end)

        state = compact(state, source_level, target_level, retry - 1)
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp process_level_after_compaction(levels, level_key, old_file) do
    level = Map.get(levels, level_key)
    level_entries = Map.delete(level.entries, old_file)
    %{level | entries: level_entries, compacting_ref: nil}
  end

  defp maybe_compact(state, source_level_key) do
    source_level = Map.get(state.levels, source_level_key)

    target_level =
      Map.get(state.levels, source_level_key + 1, %Level{level_key: source_level_key + 1})

    cond do
      compacting?(source_level, target_level) ->
        state

      exceeding_level_limit?(source_level, state.level_limit) ->
        compact(state, source_level, target_level)

      true ->
        state
    end
  end

  defp compacting?(%{compacting_ref: nil}, %{compacting_ref: nil}), do: false
  defp compacting?(_source_level, _target_level), do: true

  defp exceeding_level_limit?(source_level, level_limit) do
    Level.get_total_size(source_level) >= level_limit(level_limit, source_level.level_key)
  end

  defp compact(state, source_level, target_level, retry \\ 5) do
    %{
      levels: levels,
      key_limit: key_limit,
      store: store,
      manifest: manifest,
      rw_locks: rw_locks,
      task_sup: task_sup,
      task_mod: task_mod
    } = state

    source_level_key = source_level.level_key

    sources =
      source_level
      |> Level.get_highest_prio_entries()
      |> Enum.map(& &1.id)

    clean_tombstones? = clean_tombstones?(target_level, levels)

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        with {:ok, old, new} <-
               SSTs.merge(
                 sources,
                 target_level,
                 key_limit,
                 clean_tombstones?,
                 &Level.place_in_buffer(&2, &1),
                 fn -> Store.new_file(store) end
               ),
             :ok <- Manifest.log_compaction(manifest, sources ++ old, Enum.map(new, &elem(&1, 0))),
             :ok <- put_new_files_in_store(new, target_level.level_key, store),
             :ok <- remove_old_files(sources ++ old, store, rw_locks) do
          {:ok, sources ++ old, source_level_key, target_level.level_key}
        end
      end)

    source_level = %{source_level | compacting_ref: {ref, retry}}
    target_level = %{target_level | compacting_ref: {ref, retry}}

    levels =
      state.levels
      |> Map.put(source_level.level_key, source_level)
      |> Map.put(target_level.level_key, target_level)

    %{state | levels: levels}
  end

  defp put_new_files_in_store([], _level_key, _store), do: :ok

  defp put_new_files_in_store(
         [{file, {bloom_filter, priority, size, key_range}} | new],
         level_key,
         store
       ) do
    Store.put(store, file, level_key, bloom_filter, priority, size, key_range)
    put_new_files_in_store(new, level_key, store)
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
