defmodule Goblin.Compactor do
  @moduledoc false
  use GenServer
  alias Goblin.SSTs
  alias Goblin.Store
  alias Goblin.Manifest
  alias Goblin.Reader

  @flush_level 0

  defstruct [
    :bf_fpp,
    :manifest,
    :store,
    :reader,
    :task_sup,
    :task_mod,
    :key_limit,
    :level_limit,
    :compacting,
    levels: %{}
  ]

  @typep compactor :: module() | {:via, Registry, {module(), module()}}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [
        :bf_fpp,
        :store,
        :manifest,
        :reader,
        :key_limit,
        :level_limit,
        :task_sup,
        :task_mod
      ])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(
          compactor(),
          Goblin.db_level_key(),
          Goblin.db_file(),
          Goblin.seq_no(),
          non_neg_integer(),
          {Goblin.db_key(), Goblin.db_key()}
        ) :: :ok
  def put(compactor, level_key, file, priority, size, key_range) do
    GenServer.call(compactor, {:put, level_key, file, priority, size, key_range})
  end

  @spec is_compacting(compactor()) :: boolean()
  def is_compacting(compactor) do
    GenServer.call(compactor, :is_compacting)
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       bf_fpp: args[:bf_fpp],
       store: args[:store],
       manifest: args[:manifest],
       reader: args[:reader],
       key_limit: args[:key_limit],
       level_limit: args[:level_limit],
       task_sup: args[:task_sup],
       task_mod: args[:task_mod] || Task.Supervisor
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

  def handle_call(:is_compacting, _from, %{compacting: nil} = state),
    do: {:reply, false, state}

  def handle_call(:is_compacting, _from, state),
    do: {:reply, true, state}

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
    %{compacting: {_, source_clean_ups, target_clean_ups}} = state

    levels =
      state.levels
      |> clean_up(source_clean_ups)
      |> clean_up(target_clean_ups)

    state = %{state | compacting: nil, levels: levels}
    {:noreply, state, {:continue, :maybe_compact}}
  end

  def handle_info({ref, {:error, _reason} = error}, %{compacting: {ref, _, _}} = state) do
    {:stop, error, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{compacting: {ref, _, _}} = state) do
    {:stop, {:error, reason}, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp maybe_compact(state, []), do: state

  defp maybe_compact(state, [level_key | level_keys]) do
    level = Map.get(state.levels, level_key, [])

    if exceeding_level_limit?(level, level_key, state.level_limit) do
      compacting_ref = compact(state, level_key)
      %{state | compacting: compacting_ref}
    else
      maybe_compact(state, level_keys)
    end
  end

  defp compact(state, source_level_key) do
    %{
      task_mod: task_mod,
      task_sup: task_sup,
      store: store,
      manifest: manifest,
      reader: reader,
      key_limit: key_limit,
      bf_fpp: bf_fpp,
      levels: levels
    } = state

    target_level_key = source_level_key + 1

    {sources, targets} =
      if source_level_key == @flush_level do
        sources = Map.get(levels, source_level_key, [])
        targets = Map.get(levels, target_level_key, [])
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

    source_ids = Enum.map(sources, & &1.id)
    target_ids = Enum.map(targets, & &1.id)

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        data =
          (sources ++ targets)
          |> merge_stream()
          |> Stream.chunk_every(key_limit)

        opts = [
          file_getter: fn -> Store.new_file(store) end,
          bf_fpp: bf_fpp,
          compress?: false
        ]

        with {:ok, ssts} <- SSTs.new([data], target_level_key, opts),
             :ok <-
               Manifest.log_compaction(
                 manifest,
                 source_ids ++ target_ids,
                 Enum.map(ssts, & &1.file)
               ),
             :ok <- Store.put(store, ssts),
             :ok <- Reader.empty?(reader),
             :ok <- Store.remove(store, source_ids ++ target_ids),
             :ok <- remove_merged(source_ids ++ target_ids) do
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

  defp merge_stream(ssts) do
    Stream.resource(
      fn ->
        Enum.map(ssts, &{SSTs.iterate(&1.id), nil})
      end,
      fn cursors ->
        cursors =
          cursors
          |> Enum.flat_map(&jump/1)
          |> Enum.sort_by(fn {_, {key, seq, _}} -> {key, -seq} end)

        case cursors do
          [] ->
            {:halt, :ok}

          [{_, {smallest_key, _, _} = next} | _] ->
            {[next], Enum.flat_map(cursors, &skip(&1, smallest_key))}
        end
      end,
      fn _ -> :ok end
    )
  end

  defp jump({nil, data}), do: [{nil, data}]

  defp jump({iterator, nil}) do
    case SSTs.iterate(iterator) do
      :ok -> []
      {data, iterator} -> jump({iterator, data})
    end
  end

  defp jump({iterator, data}) do
    {key, _, _} = data

    case SSTs.iterate(iterator) do
      :ok -> [{nil, data}]
      {{^key, _, _} = data, iterator} -> jump({iterator, data})
      _ -> [{iterator, data}]
    end
  end

  defp skip({nil, {key, _, _}}, key), do: []
  defp skip({nil, data}, _), do: [{nil, data}]

  defp skip({iterator, {key, _, _}}, key) do
    case SSTs.iterate(iterator) do
      :ok -> []
      {next, iterator} -> [{iterator, next}]
    end
  end

  defp skip(cursor, _key), do: [cursor]

  defp remove_merged([]), do: :ok

  defp remove_merged([file | files]) do
    with :ok <- SSTs.delete(file) do
      remove_merged(files)
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

  defp exceeding_level_limit?(level, level_key, level_limit) do
    Enum.sum_by(level, & &1.size) >= level_limit * 10 ** level_key
  end
end
