defmodule SeaGoat.SSTables.FlushIterator do
  defstruct [:data]

  defimpl SeaGoat.SSTables.SSTableIterator, for: SeaGoat.SSTables.FlushIterator do
    def init(iterator, mem_table) do
      {:ok, %{iterator | data: Enum.sort(mem_table)}}
    end

    def next(%{data: []} = iterator), do: {:eod, iterator}

    def next(%{data: [next | data]} = iterator) do
      iterator = %{iterator | data: data}
      {:next, next, iterator}
    end

    def deinit(_iterator), do: :ok
  end
end
