defmodule Goblin.Merger do
  @moduledoc false

  def put_into_level(levels, level_key, table) do
    Map.update(levels, level_key, [table], &[table | &1])
  end

  def next_merge(levels) do
    case find_merge(levels) do
      nil -> nil
      {stream, levels} -> {:merge, stream, levels}
    end
  end

  defp find_merge(levels) do
    case find_overflow(levels) do
      nil -> nil
      {stream, level} -> nil
    end

    # levels
    # |> Enum.sort()
    # |> find_overflow()
    # |> case do
    #   nil ->
    #     nil
    #
    #   {level_key, stream, level} ->
    #     targets = find_target(levels, level_key, sources)
    #     stream = build_stream(sources ++ targets)
    #     {:merge, stream, Map.put(levels, level_key, level)}
    # end
  end

  defp find_overflow([]), do: nil

  defp find_overflow([{-1, [mem_table]} | levels]) do
  end

  defp find_overflow([{0, level} | levels]) do
    if length(level) > 4 do
      target_level =
        targets = find_targets()

      {0, level, []}
    else
      find_overflow(levels)
    end
  end

  defp find_overflow([{level_key, level} | levels]) when level_key > 0 do
  end
end
