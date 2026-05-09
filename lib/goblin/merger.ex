# defmodule Goblin.Merger do
#   @moduledoc false
#
#   def put_into_level(levels, level_key, table) do
#     Map.update(levels, level_key, [table], &[table | &1])
#   end
#
#   def next_merge(levels) do
#     max_lk = levels |> Map.keys() |> Enum.max()
#
#     case find_overflow(levels, max_lk) do
#       nil -> nil
#       {stream, levels} -> {:merge, stream, levels}
#     end
#   end
#
#   # defp find_merge(levels) do
#   #   case find_overflow(levels) do
#   #     nil -> nil
#   #     {stream, levels} -> {:merge, stream, levels}
#   #   end
#   #
#   #   # levels
#   #   # |> Enum.sort()
#   #   # |> find_overflow()
#   #   # |> case do
#   #   #   nil ->
#   #   #     nil
#   #   #
#   #   #   {level_key, stream, level} ->
#   #   #     targets = find_target(levels, level_key, sources)
#   #   #     stream = build_stream(sources ++ targets)
#   #   #     {:merge, stream, Map.put(levels, level_key, level)}
#   #   # end
#   # end
#
#   defp find_overflow(levels, max_lk, level_key \\ -1)
#
#   defp find_overflow(_levels, max_lk, lk) when lk > max_lk, do: nil
#
#   defp find_overflow(levels, _max_lk, -1) do
#     [mem_table] = Map.get(levels, -1)
#
#     # rotate here?
#     # When to rotate?
#     case MemTable.size(mem_table) > :mem_limit do
#       true ->
#         stream = MemTable.stream(mem_table, :infinity)
#         opts = []
#         # {stream, opts, Map.put()}
#     end
#   end
#
#   defp find_overflow(levels, _max_lk, 0) do
#   end
#
#   defp find_overflow(levels, max_lk, level_key) do
#   end
#
#   # defp find_overflow([]), do: nil
#   #
#   # defp find_overflow([{-1, [mem_table]} | levels]) do
#   # end
#   #
#   # defp find_overflow([{0, level} | levels]) do
#   #   if length(level) > 4 do
#   #     target_level =
#   #       targets = find_targets()
#   #
#   #     {0, level, []}
#   #   else
#   #     find_overflow(levels)
#   #   end
#   # end
#   #
#   # defp find_overflow([{level_key, level} | levels]) when level_key > 0 do
#   # end
# end
