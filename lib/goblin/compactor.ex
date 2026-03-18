defmodule Goblin.Compactor do
  @moduledoc false

  alias Goblin.{
    Iterator,
    DiskTable
  }

  defstruct [
    :ref,
    :compaction_opts,
    levels: %{},
    queue: :queue.new()
  ]

  @type t :: %__MODULE__{
          ref: reference() | nil,
          compaction_opts: keyword(),
          levels: %{non_neg_integer() => [DiskTable.t()]},
          queue: :queue.queue(non_neg_integer())
        }

  @doc "Creates a new compactor with the given compaction options."
  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      compaction_opts: opts
    }
  end

  @doc "Tracks a disk table in its corresponding level."
  @spec put_into_level(t(), DiskTable.t()) :: t()
  def put_into_level(compactor, disk_table) do
    levels =
      Map.update(
        compactor.levels,
        disk_table.level_key,
        [disk_table],
        &[disk_table | &1]
      )

    %{compactor | levels: levels}
  end

  @doc "Removes a disk table from its corresponding level."
  @spec remove_from_level(t(), DiskTable.t()) :: t()
  def remove_from_level(compactor, disk_table) do
    level =
      compactor.levels
      |> Map.get(disk_table.level_key, [])
      |> Enum.reject(&(&1.file == disk_table.file))

    levels = Map.put(compactor.levels, disk_table.level_key, level)
    %{compactor | levels: levels}
  end

  @doc "Checks for an overflowing level and enqueues compaction if found."
  @spec push(t()) :: {:noop, t()} | {:compact, non_neg_integer(), t()}
  def push(compactor) do
    case find_overflowing_level(compactor) do
      nil ->
        {:noop, compactor}

      level_key ->
        queue = :queue.in(level_key, compactor.queue)

        %{compactor | queue: queue}
        |> next()
    end
  end

  @doc "Dequeues the next pending compaction job."
  @spec pop(t()) :: {:noop, t()} | {:compact, non_neg_integer(), t()}
  def pop(compactor), do: next(compactor)

  @doc "Sets or clears the active compaction task reference."
  @spec set_ref(t(), reference() | nil) :: t()
  def set_ref(compactor, ref), do: %{compactor | ref: ref}

  @doc "Runs compaction for the given source level, merging into the next level."
  @spec compact(t(), non_neg_integer()) ::
          {:ok, [DiskTable.t()], [DiskTable.t()]} | {:error, term()}
  def compact(compactor, source_level_key) do
    target_level_key = source_level_key + 1

    %{
      sources: sources,
      targets: targets,
      filter_tombstones: filter_tombstones
    } = plan_compaction(compactor.levels, source_level_key, target_level_key)

    opts =
      [
        level_key: target_level_key,
        compress?: target_level_key > 1
      ] ++ compactor.compaction_opts

    stream =
      Iterator.k_merge_stream(
        fn -> Enum.map(sources ++ targets, &DiskTable.StreamIterator.new/1) end,
        filter_tombstones: filter_tombstones
      )

    with {:ok, disk_tables} <- DiskTable.into(stream, opts) do
      {:ok, disk_tables, sources ++ targets}
    end
  end

  defp plan_compaction(levels, source_level_key, target_level_key) do
    {sources, targets} = get_inputs(levels, source_level_key, target_level_key)

    %{
      sources: sources,
      targets: targets,
      filter_tombstones: target_level_key >= Enum.max(Map.keys(levels), fn -> 0 end)
    }
  end

  defp get_inputs(levels, 0, target_level_key) do
    case Map.get(levels, 0, []) do
      [] ->
        {[], []}

      sources ->
        key_range = bounding_range(sources)
        targets = find_overlapping(Map.get(levels, target_level_key, []), key_range)
        {sources, targets}
    end
  end

  defp get_inputs(levels, source_level_key, target_level_key) do
    case Map.get(levels, source_level_key, []) do
      [] ->
        {[], []}

      sources ->
        source = Enum.min_by(sources, &elem(&1.seq_range, 0))
        key_range = bounding_range([source])

        targets = find_overlapping(Map.get(levels, target_level_key, []), key_range)
        {[source], targets}
    end
  end

  defp bounding_range(sources) do
    min_key =
      sources
      |> Enum.map(&elem(&1.key_range, 0))
      |> Enum.min()

    max_key =
      sources
      |> Enum.map(&elem(&1.key_range, 1))
      |> Enum.max()

    {min_key, max_key}
  end

  defp find_overlapping(level, key_range, acc \\ [])
  defp find_overlapping([], _key_range, acc), do: acc

  defp find_overlapping([target | targets], {min, max}, acc) do
    %{key_range: {target_min, target_max}} = target

    cond do
      target_max < min -> find_overlapping(targets, {min, max}, acc)
      target_min > max -> find_overlapping(targets, {min, max}, acc)
      true -> find_overlapping(targets, {min, max}, [target | acc])
    end
  end

  defp next(%{ref: nil} = compactor) do
    case :queue.out(compactor.queue) do
      {:empty, queue} ->
        {:noop, %{compactor | queue: queue}}

      {{:value, level_key}, queue} ->
        {:compact, level_key, %{compactor | queue: queue}}
    end
  end

  defp next(compactor), do: {:noop, compactor}

  defp find_overflowing_level(compactor) do
    compactor.levels
    |> Map.keys()
    |> Enum.sort()
    |> Enum.find(&exceeding_level_limit?(compactor, &1))
  end

  defp exceeding_level_limit?(compactor, 0) do
    level = Map.get(compactor.levels, 0, [])
    Enum.count(level) >= compactor.compaction_opts[:flush_level_file_limit]
  end

  defp exceeding_level_limit?(compactor, level_key) do
    level = Map.get(compactor.levels, level_key, [])
    level_base_size = compactor.compaction_opts[:level_base_size]
    level_size_multiplier = compactor.compaction_opts[:level_size_multiplier]

    Enum.sum_by(level, & &1.size) >= level_base_size * level_size_multiplier ** (level_key - 1)
  end
end
