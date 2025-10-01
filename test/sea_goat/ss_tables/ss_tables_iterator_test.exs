defmodule SeaGoat.SSTables.SSTablesIteratorTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables
  alias SeaGoat.SSTables.Iterator
  alias SeaGoat.SSTables.SSTablesIterator
  alias SeaGoat.SSTables.MemTableIterator

  @moduletag :tmp_dir

  setup c do
    {keys, files} =
      for {name, data} <- generate_data(), reduce: {[], []} do
        {keys, files} ->
          file = Path.join(c.tmp_dir, name)
          write_ss_table(file, data)
          {Map.keys(data) ++ keys, [file | files]}
      end

    %{files: Enum.reverse(files), keys: keys |> Enum.uniq() |> Enum.sort()}
  end

  test "iterating iterates through the key-value pairs in the merging files", c do
    assert {:ok, iterator} = Iterator.init(%SSTablesIterator{}, c.files)
    {iterator, acc} = iterate(iterator)
    assert Enum.reverse(acc) == c.keys
    assert :ok == Iterator.deinit(iterator)
  end

  defp iterate(iterator, acc \\ []) do
    case Iterator.next(iterator) do
      {:next, {k, _v}, iterator} ->
        for n <- acc, do: assert(k >= n)
        iterate(iterator, [k | acc])

      {:end_iter, iterator} ->
        {iterator, acc}
    end
  end

  defp generate_data do
    data_pool = 1..100

    Enum.map(1..5, fn n ->
      name = "#{n}.seagoat"

      data =
        for _ <- 1..10, reduce: %{} do
          acc ->
            key = Enum.random(data_pool)
            value = "#{n}_#{key}"
            Map.put(acc, key, value)
        end

      {name, data}
    end)
  end

  defp write_ss_table(file, data) do
    SSTables.write(%MemTableIterator{}, data, file, 0)
  end
end
