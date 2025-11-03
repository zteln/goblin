defmodule Goblin.Compactor do
  @moduledoc false
  use GenServer
  import Goblin.ProcessRegistry, only: [via: 1]
  require Logger
  alias Goblin.Compactor.Entry
  alias Goblin.Compactor.Level
  alias Goblin.SSTs
  alias Goblin.Store
  alias Goblin.Manifest
  alias Goblin.RWLocks

  defstruct [
    :registry,
    :task_sup,
    :task_mod,
    :key_limit,
    :level_limit,
    levels: %{}
  ]

  @type data :: {
          Goblin.db_file(),
          Goblin.db_sequence(),
          non_neg_integer(),
          {Goblin.db_key(), Goblin.db_key()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    registry = opts[:registry]

    args =
      Keyword.take(opts, [
        :registry,
        :key_limit,
        :level_limit,
        :task_sup,
        :task_mod
      ])

    GenServer.start_link(__MODULE__, args, name: via(registry))
  end

  @spec put(Goblin.registry(), Goblin.db_level_key(), data()) :: :ok
  def put(registry, level_key, data) do
    GenServer.call(via(registry), {:put, level_key, data})
  end

  @spec is_compacting(Goblin.registry()) :: boolean()
  def is_compacting(registry) do
    GenServer.call(via(registry), :is_compacting)
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       registry: args[:registry],
       key_limit: args[:key_limit],
       level_limit: args[:level_limit],
       task_sup: args[:task_sup],
       task_mod: args[:task_mod] || Task.Supervisor
     }}
  end

  @impl GenServer
  def handle_call({:put, level_key, {file, priority, size, key_range}}, _from, state) do
    entry = %Entry{
      id: file,
      priority: priority,
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

  def handle_call(:is_compacting, _from, state) do
    is_compacting =
      Enum.any?(state.levels, fn {_level_key, level} ->
        level.compacting_ref
      end)

    {:reply, is_compacting, state}
  end

  @impl GenServer
  def handle_info({_ref, {:ok, compacted_away_files, source_level_key, target_level_key}}, state) do
    state =
      Enum.reduce(compacted_away_files, state, fn compacted_away_file, acc ->
        levels =
          acc.levels
          |> process_level_after_compaction(source_level_key, compacted_away_file)
          |> process_level_after_compaction(target_level_key, compacted_away_file)

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

  defp process_level_after_compaction(levels, level_key, old_file)
       when is_map_key(levels, level_key) do
    level = Map.get(levels, level_key)
    level_entries = Map.delete(level.entries, old_file)

    if map_size(level_entries) == 0 do
      Map.delete(levels, level_key)
    else
      level = %{level | entries: level_entries, compacting_ref: nil}
      Map.put(levels, level_key, level)
    end
  end

  defp process_level_after_compaction(levels, _, _), do: levels

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
      registry: registry,
      levels: levels,
      key_limit: key_limit,
      task_sup: task_sup,
      task_mod: task_mod
    } = state

    source_level_key = source_level.level_key

    sources =
      source_level
      |> Level.get_highest_prio_entries()
      |> Enum.map(& &1.id)

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        %{
          entries: entries,
          level_key: level_key
        } = target_level = deplete(sources, target_level)

        levels = Map.put(levels, level_key, target_level)
        clean_tombstones? = clean_tombstones?(target_level, levels)
        old = Map.keys(entries)

        data =
          Enum.map(entries, fn {id, %{buffer: buffer}} ->
            buffer =
              if clean_tombstones? do
                Enum.reject(buffer, fn {_, {_, v}} -> v == :tombstone end)
              else
                buffer
              end

            {id, buffer}
          end)
          |> Enum.map(fn {id, buffer} ->
            merge_stream(id, buffer, key_limit)
          end)

        with {:ok, new} <-
               SSTs.new(data, level_key, fn -> Store.new_file(registry) end),
             :ok <- Manifest.log_compaction(registry, sources ++ old, Enum.map(new, & &1.file)),
             :ok <- put_new_files_in_store(new, target_level.level_key, registry),
             :ok <- remove_old_files(sources ++ old, registry) do
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

  defp deplete([], level), do: level

  defp deplete([source | sources], level) do
    level =
      source
      |> SSTs.stream!()
      |> Enum.reduce(level, &Level.place_in_buffer(&2, &1))

    deplete(sources, level)
  end

  defp merge_stream(id, buffer, key_limit) do
    Stream.resource(
      fn ->
        sst_iter = init_sst_iter(id)
        buffer = init_buffer(buffer)
        {sst_iter, buffer, nil}
      end,
      &iter_merge_data/1,
      fn _ -> :ok end
    )
    |> Stream.chunk_every(key_limit)
  end

  defp init_sst_iter(nil), do: nil
  defp init_sst_iter(id), do: SSTs.iterate(id)

  defp init_buffer(buffer) do
    buffer
    |> Enum.map(fn {key, {seq, value}} -> {seq, key, value} end)
    |> Enum.sort_by(fn {_seq, key, _value} -> key end, :asc)
  end

  defp iter_merge_data({nil, [], nil}), do: {:halt, :ok}
  defp iter_merge_data({nil, [next | buffer], nil}), do: {[next], {nil, buffer, nil}}

  defp iter_merge_data({iter, [], nil}) do
    case SSTs.iterate(iter) do
      :ok -> {:halt, :ok}
      {data, iter} -> {[data], {iter, [], nil}}
    end
  end

  defp iter_merge_data({iter, [], placeholder}), do: {[placeholder], {iter, [], nil}}

  defp iter_merge_data({iter, [next | buffer], nil}) do
    case SSTs.iterate(iter) do
      :ok -> {[next], {nil, buffer, nil}}
      {data, iter} -> iter_merge_data({iter, [next | buffer], data})
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

  defp put_new_files_in_store([], _level_key, _store), do: :ok

  defp put_new_files_in_store([sst | ssts], level_key, registry) do
    Store.put(registry, sst)
    put_new_files_in_store(ssts, level_key, registry)
  end

  defp remove_old_files([], _registry), do: :ok

  defp remove_old_files([file | old], registry) do
    with :ok <- Store.remove(registry, file),
         :ok <- RWLocks.wlock(registry, file),
         :ok <- SSTs.delete(file),
         :ok <- RWLocks.unlock(registry, file) do
      remove_old_files(old, registry)
    end
  end

  defp level_limit(level_limit, level_key), do: level_limit * :math.pow(10, level_key)

  defp clean_tombstones?(target_level, levels) do
    has_virtual_entry(target_level) and max_level(levels) == target_level.level_key
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
