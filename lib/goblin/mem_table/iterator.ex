defmodule Goblin.MemTable.Iterator do
  @moduledoc false
  defstruct [
    :idx,
    :store,
    :max_seq
  ]

  defimpl Goblin.Iterable do
    alias Goblin.MemTable.Store

    def init(iterator), do: iterator

    def next(%{idx: nil} = iterator) do
      idx = Store.iterate(iterator.store)
      handle_iteration(iterator, idx)
    end

    def next(iterator) do
      idx = Store.iterate(iterator.store, iterator.idx)
      handle_iteration(iterator, idx)
    end

    def close(_iterator), do: :ok

    defp handle_iteration(_iterator, :end_of_iteration), do: :ok

    defp handle_iteration(%{max_seq: max_seq} = iterator, {key, seq} = idx)
         when seq < max_seq do
      triple = Store.get(iterator.store, key, seq)
      {triple, %{iterator | idx: idx}}
    end

    defp handle_iteration(iterator, idx) do
      next(%{iterator | idx: idx})
    end
  end
end
