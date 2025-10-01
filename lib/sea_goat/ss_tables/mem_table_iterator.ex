defmodule SeaGoat.SSTables.MemTableIterator do
  @moduledoc """
  Iterates through data in a sorted fashion until there is no data left.
  """
  defstruct [:data]

  defimpl SeaGoat.SSTables.Iterator do
    def init(iterator, mem_table) do
      {:ok, %{iterator | data: Enum.sort(mem_table)}}
    end

    def next(%{data: []} = iterator), do: {:end_iter, iterator}

    def next(%{data: [next | data]} = iterator) do
      iterator = %{iterator | data: data}
      {:next, next, iterator}
    end

    def deinit(_iterator), do: :ok
  end
end
