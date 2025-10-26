# defmodule Talon.Writer.FlushQueue do
#   @moduledoc false
#   @type t :: :queue.queue()
#
#   @spec new() :: t()
#   def new, do: :queue.new()
#
#   @spec push(t(), term(), term()) :: t()
#   def push(queue, data, extra) do
#     :queue.in({data, extra}, queue)
#   end
#
#   @spec pop(t()) :: {term(), term(), t()}
#   def pop(queue) do
#     case :queue.out(queue) do
#       {:empty, queue} ->
#         queue
#
#       {{:value, {data, extra}}, queue} ->
#         {data, extra, queue}
#     end
#   end
#
#   @spec empty?(t()) :: boolean()
#   def empty?(queue), do: :queue.len(queue) == 0
#
#   @spec size(t()) :: non_neg_integer()
#   def size(queue), do: :queue.len(queue)
#
#   @spec to_data_list(t()) :: [term()]
#   def to_data_list(queue) do
#     queue
#     |> :queue.to_list()
#     |> Enum.map(fn {data, _extra} -> data end)
#     |> Enum.reverse()
#   end
# end
