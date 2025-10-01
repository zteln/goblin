defmodule SeaGoat.SSTables.MemTableIteratorTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables.Iterator
  alias SeaGoat.SSTables.MemTableIterator

  test "iterates through data in a sorted manner" do
    data =
      for n <- 1..10 do
        {n, "v-#{n}"}
      end
      |> Enum.shuffle()

    assert {:ok, iterator} = Iterator.init(%MemTableIterator{}, data)

    assert :ok ==
             1..11
             |> Enum.reduce_while(iterator, fn n, iterator ->
               case Iterator.next(iterator) do
                 {:next, next, iterator} ->
                   assert {n, "v-#{n}"} == next
                   {:cont, iterator}

                 {:end_iter, iterator} ->
                   {:halt, Iterator.deinit(iterator)}
               end
             end)
  end
end
