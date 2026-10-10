defmodule Goblin.Compaction do
  @moduledoc false

  @spec next(list(Goblin.DiskTable.t()), keyword()) ::
          nil | {:merge, non_neg_integer(), list(Goblin.DiskTable.t()), boolean()}
  def next(tables, opts) do
    levels =
      tables
      |> Enum.filter(&(&1.level_key >= 0))
      |> Enum.group_by(& &1.level_key)

    case find_overflowing_level(levels, opts) do
      nil ->
        nil

      {source_lk, sources} ->
        target_lk = source_lk + 1
        targets = get_targets(levels, target_lk, sources)
        filter_tombstones? = target_lk >= Enum.max(Map.keys(levels), fn -> 0 end)
        {:merge, target_lk, sources ++ targets, filter_tombstones?}
    end
  end

  defp get_targets(levels, lk, sources) do
    key_range = bounding_range(sources)

    levels
    |> Map.get(lk, [])
    |> find_overlapping(key_range)
  end

  defp find_overflowing_level(levels, opts) do
    levels
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.find_value(&check_level(&1, opts))
  end

  defp check_level({0, level}, opts) do
    limit = opts[:flush_level_file_limit]
    if length(level) >= limit, do: {0, level}
  end

  defp check_level({lk, level}, opts) do
    level_base_size = opts[:level_base_size]
    level_size_multiplier = opts[:level_size_multiplier]

    if Enum.sum_by(level, & &1.size) >= level_base_size * level_size_multiplier ** (lk - 1),
      do: {lk, [Enum.min_by(level, &elem(&1.sqn_range, 0))]}
  end

  defp bounding_range(dts) do
    %{key_range: {min, _}} = Enum.min_by(dts, &elem(&1.key_range, 0))
    %{key_range: {_, max}} = Enum.max_by(dts, &elem(&1.key_range, 1))
    {min, max}
  end

  defp find_overlapping(dts, key_range, acc \\ [])
  defp find_overlapping([], _key_range, acc), do: acc

  defp find_overlapping([dt | dts], {min, max}, acc) do
    %{key_range: {dt_min, dt_max}} = dt

    cond do
      dt_max < min -> find_overlapping(dts, {min, max}, acc)
      dt_min > max -> find_overlapping(dts, {min, max}, acc)
      true -> find_overlapping(dts, {min, max}, [dt | acc])
    end
  end
end
