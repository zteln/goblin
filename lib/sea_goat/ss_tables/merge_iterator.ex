defmodule SeaGoat.SSTables.MergeIterator do
  defstruct [:ss_tables]

  defimpl SeaGoat.SSTables.SSTableIterator, for: SeaGoat.SSTables.MergeIterator do
    def init(iterator, paths) do
      with {:ok, ss_tables} <- open_ss_tables(paths) do
        {:ok, %{iterator | ss_tables: ss_tables}}
      end
    end

    def next(%{ss_tables: []} = iterator), do: {:eod, iterator}

    def next(iterator) do
      {k, v} =
        iterator.ss_tables
        |> Enum.map(&elem(&1, 2))
        |> take_smallest_kv()

      {:ok, ss_tables} = advance(iterator.ss_tables, k)
      {:next, {k, v}, %{iterator | ss_tables: ss_tables}}
    end

    def deinit(iterator) do
      close_ss_tables(iterator.ss_tables)
    end

    defp open_ss_tables(paths, acc \\ [])
    defp open_ss_tables([], acc), do: {:ok, acc}

    defp open_ss_tables([path | paths], acc) do
      starting_offset = 0

      with {:ok, io, ^starting_offset} <- SeaGoat.SSTable.Disk.open(path, start?: true),
           {:ok, offset, kv} <- next_kv(io, starting_offset) do
        open_ss_tables(paths, [{io, offset, kv, path} | acc])
      end
    end

    defp close_ss_tables([]), do: :ok

    defp close_ss_tables([{io, _, _, _} | ss_tables]) do
      with :ok <- SeaGoat.SSTable.Disk.close(io) do
        close_ss_tables(ss_tables)
      end
    end

    defp take_smallest_kv(kvs) do
      kvs
      |> Enum.reduce(hd(kvs), fn {k, v}, {smallest_key, _} = smallest_kv ->
        if k < smallest_key do
          {k, v}
        else
          smallest_kv
        end
      end)
    end

    defp advance(open_ss_tables, key, acc \\ [])
    defp advance([], _key, acc), do: {:ok, Enum.reverse(acc)}

    defp advance([{io, offset, {key, _v}, ss_table} | open_ss_tables], key, acc) do
      with {:ok, offset, kv} <- next_kv(io, offset) do
        acc = if kv, do: [{io, offset, kv, ss_table} | acc], else: acc
        advance(open_ss_tables, key, acc)
      end
    end

    defp advance([open_ss_table | open_ss_tables], key, acc) do
      advance(open_ss_tables, key, [open_ss_table | acc])
    end

    defp next_kv(io, offset) do
      with {:ok, encoded_block_header} <-
             SeaGoat.SSTable.Disk.read(io, offset, SeaGoat.SSTables.SSTable.size(:block_header)),
           {:ok, span} <- SeaGoat.SSTables.SSTable.block_span(encoded_block_header),
           {:ok, encoded_block} <-
             SeaGoat.SSTable.Disk.read(io, offset, SeaGoat.SSTables.SSTable.size(:block) * span),
           {:ok, pair} <- SeaGoat.SSTables.SSTable.decode_block(encoded_block) do
        {:ok, offset + SeaGoat.SSTables.SSTable.size(:block) * span, pair}
      else
        {:error, :eod} ->
          {:ok, offset, nil}

        e ->
          e
      end
    end
  end
end
