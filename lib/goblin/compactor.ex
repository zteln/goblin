defmodule Goblin.Compactor do
  @moduledoc false
  use GenServer
  alias Goblin.DiskTables
  alias Goblin.Manifest
  alias Goblin.Cleaner

  @flush_level 0

  defstruct [
    :manifest_server,
    :disk_tables_server,
    :cleaner_server,
    :flush_level_file_limit,
    :level_base_size,
    :level_size_multiplier,
    :compacting,
    levels: %{}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [
        :manifest_server,
        :disk_tables_server,
        :cleaner_server,
        :flush_level_file_limit,
        :level_base_size,
        :level_size_multiplier
      ])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(Goblin.server(), Goblin.DiskTables.DiskTable.t()) :: :ok
  def put(server, disk_table) do
    %{
      level_key: level_key,
      file: file,
      seq_range: {priority, _},
      size: size,
      key_range: key_range
    } = disk_table

    GenServer.call(server, {:put, level_key, file, priority, size, key_range})
  end

  @spec is_compacting?(Goblin.server()) :: boolean()
  def is_compacting?(server) do
    GenServer.call(server, :is_compacting?)
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       manifest_server: args[:manifest_server],
       disk_tables_server: args[:disk_tables_server],
       cleaner_server: args[:cleaner_server],
       flush_level_file_limit: args[:flush_level_file_limit],
       level_base_size: args[:level_base_size],
       level_size_multiplier: args[:level_size_multiplier]
     }}
  end

  @impl GenServer
  def handle_call({:put, level_key, file, priority, size, key_range}, _from, state) do
    entry = %{
      id: file,
      priority: priority,
      size: size,
      key_range: key_range
    }

    levels = Map.update(state.levels, level_key, [entry], &[entry | &1])
    state = %{state | levels: levels}

    {:reply, :ok, state, {:continue, :maybe_compact}}
  end

  def handle_call(:is_compacting?, _from, state) do
    {:reply, state.compacting != nil, state}
  end

  @impl GenServer
  def handle_continue(:maybe_compact, %{compacting: nil} = state) do
    level_keys =
      state.levels
      |> Map.keys()
      |> Enum.sort()

    state = maybe_compact(state, level_keys)
    {:noreply, state}
  end

  def handle_continue(:maybe_compact, state), do: {:noreply, state}

  @impl GenServer
  def handle_info({ref, {:ok, :compacted}}, %{compacting: {ref, _, _}} = state) do
    %{
      compacting: {_, source_clean_ups, target_clean_ups},
      cleaner_server: cleaner_server
    } = state

    {_, source_ids} = source_clean_ups
    {_, target_ids} = target_clean_ups

    levels =
      state.levels
      |> clean_up(source_clean_ups)
      |> clean_up(target_clean_ups)

    Cleaner.clean(cleaner_server, source_ids ++ target_ids, true)

    state = %{state | compacting: nil, levels: levels}
    {:noreply, state, {:continue, :maybe_compact}}
  end

  def handle_info({ref, {:error, reason}}, %{compacting: {ref, _, _}} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{compacting: {ref, _, _}} = state) do
    {:stop, {:error, reason}, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp maybe_compact(state, []), do: state

  defp maybe_compact(state, [level_key | level_keys]) do
    level = Map.get(state.levels, level_key, [])

    if exceeding_level_limit?(
         level,
         level_key,
         state.flush_level_file_limit,
         state.level_base_size,
         state.level_size_multiplier
       ) do
      compacting_ref = compact(state, level_key)
      %{state | compacting: compacting_ref}
    else
      maybe_compact(state, level_keys)
    end
  end

  defp compact(state, source_level_key) do
    %{
      manifest_server: manifest_server,
      disk_tables_server: disk_tables_server,
      levels: levels
    } = state

    target_level_key = source_level_key + 1

    {sources, targets} =
      if source_level_key == @flush_level do
        sources = Map.get(levels, source_level_key, [])

        min_source_key =
          sources
          |> Enum.map(&elem(&1.key_range, 0))
          |> Enum.min()

        max_source_key =
          sources
          |> Enum.map(&elem(&1.key_range, 1))
          |> Enum.max()

        targets =
          levels
          |> Map.get(target_level_key, [])
          |> find_overlapping({min_source_key, max_source_key})

        {sources, targets}
      else
        source =
          levels
          |> Map.get(source_level_key, [])
          |> Enum.min_by(& &1.priority)

        targets =
          levels
          |> Map.get(target_level_key, [])
          |> find_overlapping(source.key_range)

        {[source], targets}
      end

    filter_tombstones = target_level_key >= Enum.max(Map.keys(levels), fn -> 0 end)

    source_ids = Enum.map(sources, & &1.id)
    target_ids = Enum.map(targets, & &1.id)

    %{ref: ref} =
      Task.async(fn ->
        opts = [
          level_key: target_level_key,
          compress?: target_level_key > 1
        ]

        iterators = Enum.map(sources ++ targets, &DiskTables.iterator(&1.id))
        stream = Goblin.Iterator.k_merge_stream(iterators, filter_tombstones: filter_tombstones)

        with {:ok, disk_tables} <- DiskTables.new(disk_tables_server, stream, opts),
             :ok <-
               Manifest.log_compaction(
                 manifest_server,
                 source_ids ++ target_ids,
                 disk_tables
               ) do
          {:ok, :compacted}
        end
      end)

    {ref, {source_level_key, source_ids}, {target_level_key, target_ids}}
  end

  defp find_overlapping(targets, key_range, acc \\ [])
  defp find_overlapping([], _key_range, acc), do: acc

  defp find_overlapping([target | targets], {min, max}, acc) do
    %{key_range: {target_min, target_max}} = target

    cond do
      target_max < min -> find_overlapping(targets, {min, max}, acc)
      target_min > max -> find_overlapping(targets, {min, max}, acc)
      true -> find_overlapping(targets, {min, max}, [target | acc])
    end
  end

  defp clean_up(levels, {level_key, ids}) do
    level =
      Map.get(levels, level_key, [])
      |> Enum.reject(&(&1.id in ids))

    if level == [] do
      Map.delete(levels, level_key)
    else
      Map.put(levels, level_key, level)
    end
  end

  defp exceeding_level_limit?(
         level,
         @flush_level,
         flush_level_file_limit,
         _level_base_size,
         _level_size_multiplier
       ) do
    Enum.count(level) >= flush_level_file_limit
  end

  defp exceeding_level_limit?(
         level,
         level_key,
         _flush_level_file_limit,
         level_base_size,
         level_size_multiplier
       ) do
    Enum.sum_by(level, & &1.size) >= level_base_size * level_size_multiplier ** (level_key - 1)
  end
end
