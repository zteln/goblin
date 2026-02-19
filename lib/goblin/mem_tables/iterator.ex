defmodule Goblin.MemTables.Iterator do
  @moduledoc false
  defstruct [
    :idx,
    :mem_table,
    :max_seq
  ]

  defimpl Goblin.Iterable do
    alias Goblin.MemTables.MemTable

    def init(iterator), do: iterator

    def deinit(_iterator), do: :ok

    def next(%{idx: nil} = iterator) do
      idx = MemTable.iterate(iterator.mem_table)
      handle_iteration(iterator, idx)
    end

    def next(iterator) do
      idx = MemTable.iterate(iterator.mem_table, iterator.idx)
      handle_iteration(iterator, idx)
    end

    defp handle_iteration(_iterator, :end_of_iteration), do: :ok

    defp handle_iteration(%{max_seq: max_seq} = iterator, {key, seq} = idx)
         when seq < max_seq do
      case MemTable.get(iterator.mem_table, key, seq) do
        :not_found -> next(%{iterator | idx: idx})
        triple -> {triple, %{iterator | idx: idx}}
      end
    end

    defp handle_iteration(iterator, idx) do
      next(%{iterator | idx: idx})
    end
  end
end
