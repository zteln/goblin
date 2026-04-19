defmodule Goblin.Compactor do
  @moduledoc false

  alias Goblin.{
    Iterator,
    Disk
  }

  defstruct [
    :opts,
    levels: %{},
    queue: :queue.new()
  ]

  @type t :: %__MODULE__{
          opts: keyword(),
          levels: %{non_neg_integer() => [Disk.Table.t()]},
          queue: :queue.queue(non_neg_integer())
        }

  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{opts: opts}
  end

  @spec put_into_level(t(), Disk.Table.t()) :: t()
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

  @spec next(t()) ::
          {:noop, t()}
          | {
              :compact,
              non_neg_integer(),
              list(Disk.Table.t()),
              list(Disk.Table.t()),
              boolean(),
              t()
            }
  def next(compactor) do
    case find_overflowing_level(compactor) do
      nil ->
        {:noop, compactor}

      source_level_key ->
        target_level_key = source_level_key + 1

        {source_dts, target_dts, filter_tombstones?} =
          plan_compaction(compactor.levels, source_level_key, target_level_key)

        compactor =
          compactor
          |> remove_from_level(source_level_key, source_dts)
          |> remove_from_level(target_level_key, target_dts)

        {:compact, target_level_key, source_dts, target_dts, filter_tombstones?, compactor}
    end
  end

  @spec compact(t(), non_neg_integer(), list(Disk.Table.t()), list(Disk.Table.t()), boolean()) ::
          {:ok, [Disk.Table.t()], [Disk.Table.t()]} | {:error, term()}
  def compact(compactor, target_level_key, source_dts, target_dts, filter_tombstones?) do
    opts =
      [
        level_key: target_level_key,
        compress?: target_level_key > 1
      ] ++ compactor.opts

    stream =
      Iterator.k_merge_stream(
        fn -> Enum.map(source_dts ++ target_dts, &Disk.StreamIterator.new/1) end,
        filter_tombstones?: filter_tombstones?
      )

    with {:ok, disk_tables} <- Disk.into_table(stream, opts) do
      {:ok, disk_tables, source_dts ++ target_dts}
    end
  end

  defp remove_from_level(compactor, level_key, disk_tables) do
    level =
      Enum.reduce(disk_tables, Map.get(compactor.levels, level_key, []), fn dt, acc ->
        Enum.reject(acc, &(&1 == dt))
      end)

    levels = Map.put(compactor.levels, level_key, level)
    %{compactor | levels: levels}
  end

  defp plan_compaction(levels, source_level_key, target_level_key) do
    {source_dts, target_dts} = get_inputs(levels, source_level_key, target_level_key)
    filter_tombstones? = target_level_key >= Enum.max(Map.keys(levels), fn -> 0 end)
    {source_dts, target_dts, filter_tombstones?}
  end

  defp get_inputs(levels, 0, target_level_key) do
    case Map.get(levels, 0, []) do
      [] ->
        {[], []}

      source_dts ->
        key_range = bounding_range(source_dts)
        target_dts = find_overlapping(Map.get(levels, target_level_key, []), key_range)
        {source_dts, target_dts}
    end
  end

  defp get_inputs(levels, source_level_key, target_level_key) do
    case Map.get(levels, source_level_key, []) do
      [] ->
        {[], []}

      source_dts ->
        source_dt = Enum.min_by(source_dts, &elem(&1.seq_range, 0))
        key_range = bounding_range([source_dt])

        target_dts = find_overlapping(Map.get(levels, target_level_key, []), key_range)
        {[source_dt], target_dts}
    end
  end

  defp bounding_range(source_dts) do
    min_key =
      source_dts
      |> Enum.map(&elem(&1.key_range, 0))
      |> Enum.min()

    max_key =
      source_dts
      |> Enum.map(&elem(&1.key_range, 1))
      |> Enum.max()

    {min_key, max_key}
  end

  defp find_overlapping(level, key_range, acc \\ [])
  defp find_overlapping([], _key_range, acc), do: acc

  defp find_overlapping([target_dt | target_dts], {min, max}, acc) do
    %{key_range: {target_min, target_max}} = target_dt

    cond do
      target_max < min -> find_overlapping(target_dts, {min, max}, acc)
      target_min > max -> find_overlapping(target_dts, {min, max}, acc)
      true -> find_overlapping(target_dts, {min, max}, [target_dt | acc])
    end
  end

  defp find_overflowing_level(compactor) do
    compactor.levels
    |> Map.keys()
    |> Enum.sort()
    |> Enum.find(&exceeding_level_limit?(compactor, &1))
  end

  defp exceeding_level_limit?(compactor, 0) do
    level = Map.get(compactor.levels, 0)
    Enum.count(level) >= compactor.opts[:flush_level_file_limit]
  end

  defp exceeding_level_limit?(compactor, level_key) do
    level = Map.get(compactor.levels, level_key)
    level_base_size = compactor.opts[:level_base_size]
    level_size_multiplier = compactor.opts[:level_size_multiplier]

    Enum.sum_by(level, & &1.size) >= level_base_size * level_size_multiplier ** (level_key - 1)
  end
end
