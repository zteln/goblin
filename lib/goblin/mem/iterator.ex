defmodule Goblin.Mem.Iterator do
  @moduledoc false

  defstruct [
    :idx,
    :table,
    :max_seq
  ]

  @type t :: %__MODULE__{
          idx: term() | nil,
          table: Goblin.Mem.Table.t(),
          max_seq: non_neg_integer() | nil
        }

  def new(mem, seq), do: %__MODULE__{table: mem.table, max_seq: seq}

  defimpl Goblin.Iterable do
    alias Goblin.Mem.Table

    def init(iterator), do: iterator

    def deinit(_iterator), do: :ok

    def next(%{idx: nil} = iterator) do
      idx = Table.iterate(iterator.table)
      handle_iteration(iterator, idx)
    end

    def next(iterator) do
      idx = Table.iterate(iterator.table, iterator.idx)
      handle_iteration(iterator, idx)
    end

    defp handle_iteration(_iterator, :end_of_iteration), do: :ok

    defp handle_iteration(%{max_seq: max_seq} = iterator, {key, seq} = idx)
         when seq < max_seq do
      case Table.get(iterator.table, key, seq) do
        :not_found -> next(%{iterator | idx: idx})
        triple -> {triple, %{iterator | idx: idx}}
      end
    end

    defp handle_iteration(iterator, idx) do
      next(%{iterator | idx: idx})
    end
  end
end
