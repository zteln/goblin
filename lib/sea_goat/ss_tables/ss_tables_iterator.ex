defmodule SeaGoat.SSTables.SSTablesIterator do
  @moduledoc """
  Iterates through on-disk SSTables where each iteration returns the smallest key stored.
  """
  alias SeaGoat.SSTables.Disk
  alias SeaGoat.SSTables.SSTable

  defstruct [:ss_tables]

  defimpl SeaGoat.SSTables.Iterator do
    def init(iterator, files) do
      with {:ok, ss_tables} <- open_ss_tables(files) do
        {:ok, %{iterator | ss_tables: ss_tables}}
      end
    end

    def next(%{ss_tables: []} = iterator), do: {:end_iter, iterator}

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

    defp open_ss_tables(files, acc \\ [])
    defp open_ss_tables([], acc), do: {:ok, acc}

    defp open_ss_tables([file | files], acc) do
      with {:ok, disk} <- Disk.open(file, start?: true),
           {:ok, disk, kv} <- next_kv(disk) do
        open_ss_tables(files, [{disk, kv, file} | acc])
      end
    end

    defp close_ss_tables([]), do: :ok

    defp close_ss_tables([{disk, _, _} | ss_tables]) do
      with :ok <- Disk.close(disk) do
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
             Disk.read(disk, SSTable.size(:block_header)),
           {:ok, span} <- SSTable.block_span(encoded_block_header),
           {:ok, encoded_block} <-
             Disk.read(disk, SSTable.size(:block) * span),
           {:ok, pair} <- SSTable.decode_block(encoded_block) do
        {:ok,
         Disk.advance_offset(
           disk,
           SSTable.size(:block) * span
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
