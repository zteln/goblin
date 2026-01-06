defmodule Goblin.Broker.WriteTx do
  @moduledoc false
  alias Goblin.Broker
  alias Goblin.MemTable
  alias Goblin.DiskTables

  defstruct [
    :seq,
    :mem_table,
    :disk_tables,
    writes: []
  ]

  defmodule Iterator do
    @moduledoc false

    defstruct [
      :writes
    ]

    defimpl Goblin.Iterable do
      def init(iterator), do: iterator

      def next(%{writes: []}), do: :ok

      def next(iterator) do
        [next | writes] = iterator.writes
        {next, %{iterator | writes: writes}}
      end

      def close(_iterator), do: :ok
    end
  end

  @spec new(Goblin.table(), Goblin.table()) :: Goblin.Tx.t()
  def new(mem_table, disk_tables) do
    seq = MemTable.commit_seq(mem_table)
    %__MODULE__{seq: seq, mem_table: mem_table, disk_tables: disk_tables}
  end

  @spec iterator([Goblin.write_term()]) :: Goblin.Iterable.t()
  def iterator(writes) do
    writes =
      writes
      |> Enum.map(fn
        {:put, seq, key, value} -> {key, seq, value}
        {:remove, seq, key} -> {key, seq, :"$goblin_tombstone"}
      end)
      |> Enum.sort_by(fn {key, seq, _value} -> {key, -seq} end)

    %Iterator{writes: writes}
  end

  defimpl Goblin.Tx do
    def put(tx, key, value) do
      write = {:put, tx.seq, key, value}
      %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
    end

    def put_multi(tx, pairs) do
      Enum.reduce(pairs, tx, fn {key, value}, acc ->
        write = {:put, acc.seq, key, value}
        %{acc | seq: acc.seq + 1, writes: [write | acc.writes]}
      end)
    end

    def remove(tx, key) do
      write = {:remove, tx.seq, key}
      %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
    end

    def remove_multi(tx, keys) do
      Enum.reduce(keys, tx, fn key, acc ->
        write = {:remove, acc.seq, key}
        %{acc | seq: acc.seq + 1, writes: [write | acc.writes]}
      end)
    end

    def get(tx, key, default) do
      case Enum.find(tx.writes, fn
             {:put, _seq, ^key, _value} -> true
             {:remove, _seq, ^key} -> true
             _ -> false
           end) do
        nil ->
          case Broker.ReadTx.get(tx.mem_table, tx.disk_tables, tx.seq, key) do
            :not_found -> default
            {_key, _seq, :"$goblin_tombstone"} -> default
            {_key, _seq, value} -> value
          end

        {:put, _seq, _key, value} ->
          value

        {:remove, _seq, _key} ->
          default
      end
    end

    def get_multi(tx, keys) do
      {found, keys} =
        Enum.reduce_while(tx.writes, {[], keys}, fn
          _, {_, []} = acc ->
            {:halt, acc}

          {:put, _seq, key, value}, {found, keys} = acc ->
            case Enum.split_with(keys, &(&1 == key)) do
              {[], _keys} -> {:cont, acc}
              {_, keys} -> {:cont, {[{key, value} | found], keys}}
            end

          {:remove, _seq, key}, {found, keys} = acc ->
            case Enum.split_with(keys, &(&1 == key)) do
              {[], _keys} -> {:cont, acc}
              {_, keys} -> {:cont, {found, keys}}
            end
        end)

      rest =
        Broker.ReadTx.get_multi(tx.mem_table, tx.disk_tables, tx.seq, keys)
        |> Enum.flat_map(fn
          {_k, _s, :"$goblin_tombstone"} -> []
          {k, _s, v} -> [{k, v}]
        end)

      (found ++ rest)
      |> List.keysort(0)
    end

    def select(tx, opts) do
      min = opts[:min]
      max = opts[:max]

      [
        Broker.WriteTx.iterator(tx.writes),
        MemTable.iterator(tx.mem_table, tx.seq)
        | DiskTables.stream_iterators(tx.disk_tables, min, max, tx.seq)
      ]
      |> Goblin.Iterator.k_merge_stream(min: min, max: max)
      |> Stream.map(fn {k, _s, v} -> {k, v} end)
    end
  end
end
