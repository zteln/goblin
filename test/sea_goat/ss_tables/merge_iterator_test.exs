defmodule SeaGoat.SSTables.MergeIteratorTest do
  use ExUnit.Case, async: true
  alias SeaGoat.SSTables
  alias SeaGoat.SSTables.SSTableIterator
  alias SeaGoat.SSTables.MergeIterator
  alias SeaGoat.SSTables.FlushIterator

  @moduletag :tmp_dir

  setup c do
    {keys, paths} =
      for {name, data} <- generate_data(), reduce: {[], []} do
        {keys, paths} ->
          path = Path.join(c.tmp_dir, name)
          write_ss_table(path, data)
          {Map.keys(data) ++ keys, [path | paths]}
      end

    %{paths: Enum.reverse(paths), keys: keys |> Enum.uniq() |> Enum.sort()}
  end

  test "iterating iterates through the key-value pairs in the merging paths", c do
    assert {:ok, iterator} = SSTableIterator.init(%MergeIterator{}, c.paths)
    {iterator, acc} = iterate(iterator)
    assert Enum.reverse(acc) == c.keys
    assert :ok == SSTableIterator.deinit(iterator)
  end

  defp iterate(iterator, acc \\ []) do
    case SSTableIterator.next(iterator) do
      {:next, {k, _v}, iterator} ->
        for n <- acc, do: assert(k >= n)
        iterate(iterator, [k | acc])

      {:eod, iterator} ->
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

  defp write_ss_table(path, data) do
    SSTables.write(%FlushIterator{}, data, path, 0)
  end
end
