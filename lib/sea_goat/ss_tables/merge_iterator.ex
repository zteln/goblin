defmodule SeaGoat.SSTables.MergeIterator do
  @moduledoc """
  Iterates through on-disk SSTables where each iteration returns the smallest key stored.
  """
  defstruct [:ss_tables]

  defimpl SeaGoat.SSTables.SSTableIterator do
    def init(iterator, paths) do
      with {:ok, ss_tables} <- open_ss_tables(paths) do
        {:ok, %{iterator | ss_tables: ss_tables}}
      end
    end

    def next(%{ss_tables: []} = iterator), do: {:eod, iterator}

    def next(iterator) do
      {k, v} =
        iterator.ss_tables
        |> Enum.map(&elem(&1, 1))
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
      with {:ok, disk} <- SeaGoat.SSTables.Disk.open(path, start?: true),
           {:ok, disk, kv} <- next_kv(disk) do
        open_ss_tables(paths, [{disk, kv, path} | acc])
      end
    end

    defp close_ss_tables([]), do: :ok

    defp close_ss_tables([{disk, _, _} | ss_tables]) do
      with :ok <- SeaGoat.SSTables.Disk.close(disk) do
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

    defp advance([{disk, {key, _v}, ss_table} | open_ss_tables], key, acc) do
      with {:ok, disk, kv} <- next_kv(disk) do
        acc = if kv, do: [{disk, kv, ss_table} | acc], else: acc
        advance(open_ss_tables, key, acc)
      end
    end

    defp advance([open_ss_table | open_ss_tables], key, acc) do
      advance(open_ss_tables, key, [open_ss_table | acc])
    end

    defp next_kv(disk) do
      with {:ok, encoded_block_header} <-
             SeaGoat.SSTables.Disk.read(disk, SeaGoat.SSTables.SSTable.size(:block_header)),
           {:ok, span} <- SeaGoat.SSTables.SSTable.block_span(encoded_block_header),
           {:ok, encoded_block} <-
             SeaGoat.SSTables.Disk.read(disk, SeaGoat.SSTables.SSTable.size(:block) * span),
           {:ok, pair} <- SeaGoat.SSTables.SSTable.decode_block(encoded_block) do
        {:ok,
         SeaGoat.SSTables.Disk.advance_offset(
           disk,
           SeaGoat.SSTables.SSTable.size(:block) * span
         ), pair}
      else
        {:error, :eod} ->
          {:ok, disk.offset, nil}

        e ->
          e
      end
    end
  end
end
