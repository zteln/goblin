defmodule SeaGoat.Compactor do
  use GenServer
  alias __MODULE__.Entry
  alias __MODULE__.Level
  alias SeaGoat.Actions

  defstruct [
    :store,
    :rw_locks,
    :manifest,
    :key_limit,
    :level_limit,
    levels: %{},
    merging: %{}
  ]

  @type compactor :: GenServer.server()
  @type data :: {SeaGoat.db_sequence(), non_neg_integer(), {SeaGoat.db_key(), SeaGoat.db_key()}}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:key_limit, :level_limit, :store, :rw_locks, :manifest])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(compactor(), SeaGoat.db_level_key(), SeaGoat.db_file(), data())
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
            level.level_key,
            target_level,
            clean_tombstones?,
            state.key_limit,
            {state.store, state.manifest, state.rw_locks}
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

  defp compact(file, level_key, target_level, clean_tombstones?, key_limit, pids) do
    %{ref: ref} =
      Task.async(fn ->
        Actions.merge(
          file,
          level_key,
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
