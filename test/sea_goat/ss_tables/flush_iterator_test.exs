defmodule SeaGoat.SSTables.FlushIteratorTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables.SSTableIterator
  alias SeaGoat.SSTables.FlushIterator

  test "iterates through data in a sorted manner" do
    data =
      for n <- 1..10 do
        {n, "v-#{n}"}
      end
      |> Enum.shuffle()

    assert {:ok, iterator} = SSTableIterator.init(%FlushIterator{}, data)

    assert :ok ==
             1..11
             |> Enum.reduce_while(iterator, fn n, iterator ->
               case SSTableIterator.next(iterator) do
                 {:next, next, iterator} ->
                   assert {n, "v-#{n}"} == next
                   {:cont, iterator}

                 {:eod, iterator} ->
                   {:halt, SSTableIterator.deinit(iterator)}
               end
             end)
  end
end
