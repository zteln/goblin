defmodule SeaGoatDB.Compactor do
  use GenServer
  alias __MODULE__.Entry
  alias __MODULE__.Level
  alias SeaGoatDB.Actions

  defstruct [
    :store,
    :rw_locks,
    :manifest,
    :key_limit,
    :level_limit,
    levels: %{}
  ]

  @type compactor :: GenServer.server()
  @type data ::
          {SeaGoatDB.db_sequence(), non_neg_integer(), {SeaGoatDB.db_key(), SeaGoatDB.db_key()}}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:key_limit, :level_limit, :store, :rw_locks, :manifest])
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

  def handle_info(_msg, state) do
    # Handle error, retry compaction
    {:noreply, state}
  end

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
        levels =
          start_compaction(
            state.levels,
            source_level,
            target_level,
            state.key_limit,
            {state.store, state.manifest, state.rw_locks}
          )

        %{state | levels: levels}

      true ->
        state
    end
  end

  defp compacting?(%{compacting_ref: nil}, %{compacting_ref: nil}), do: false
  defp compacting?(_source_level, _target_level), do: true

  defp exceeding_level_limit?(source_level, level_limit) do
    Level.get_total_size(source_level) >= level_limit(level_limit, source_level.level_key)
  end

  defp start_compaction(levels, source_level, target_level, key_limit, pids) do
    entries = Level.get_highest_prio_entries(source_level)
    clean_tombstones? = clean_tombstones?(target_level, levels)

    ref =
      compact(
        Enum.map(entries, & &1.id),
        source_level.level_key,
        target_level,
        clean_tombstones?,
        key_limit,
        pids
      )

    source_level = %{source_level | compacting_ref: ref}
    target_level = %{target_level | compacting_ref: ref}

    levels
    |> Map.put(source_level.level_key, source_level)
    |> Map.put(target_level.level_key, target_level)
  end

  defp compact(files, source_level_key, target_level, clean_tombstones?, key_limit, pids) do
    %{ref: ref} =
      Task.async(fn ->
        Actions.merge(
          files,
          source_level_key,
          target_level,
          &Level.place_in_buffer(&2, &1),
          clean_tombstones?,
          key_limit,
          pids
        )
      end)

    ref
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
