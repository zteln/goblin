defmodule Goblin.Broker.ReadTx do
  @moduledoc false
  alias Goblin.Broker
  alias Goblin.MemTable
  alias Goblin.DiskTables
  alias Goblin.Iterator

  defstruct [
    :seq,
    :mem_table,
    :disk_tables
  ]

  @spec new(Goblin.MemTable.Store.t(), Goblin.DiskTables.Store.t()) :: Goblin.Tx.t()
  def new(mem_table, disk_tables) do
    seq = MemTable.commit_seq(mem_table)
    %__MODULE__{seq: seq, mem_table: mem_table, disk_tables: disk_tables}
  end

  @spec get(
          Goblin.MemTable.Store.t(),
          Goblin.DiskTables.Store.t(),
          Goblin.seq_no(),
          Goblin.db_key()
        ) ::
          Goblin.triple() | :not_found
  def get(mem_table, disk_tables, seq, key) do
    case search_mem_table(mem_table, key, seq) do
      {:value, triple} ->
        triple

      :not_found ->
        case search_disk_tables(disk_tables, [key], seq) do
          [] -> :not_found
          [triple] -> triple
        end
    end
  end

  @spec get_multi(Goblin.MemTable.Store.t(), Goblin.DiskTables.Store.t(), Goblin.seq_no(), [
          Goblin.db_key()
        ]) :: [
          Goblin.triple()
        ]
  def get_multi(mem_table, disk_tables, seq, keys) do
    multi_search_mem_table(mem_table, keys, seq)
    |> Enum.split_with(fn
      {:not_found, _key} -> false
      _ -> true
    end)
    |> then(fn {found, not_found} ->
      keys = Enum.map(not_found, &elem(&1, 1))

      found =
        Enum.map(found, fn
          {:value, triple} -> triple
        end)

      found ++ search_disk_tables(disk_tables, keys, seq)
    end)
  end

  def select(iterators, min, max, tag) do
    iterators
    |> Goblin.Iterator.k_merge_stream(min: min, max: max)
    |> Stream.flat_map(fn
      {{:"$goblin_tag", ^tag, key}, _seq, value} ->
        [{tag, key, value}]

      {{:"$goblin_tag", stored_tag, key}, _seq, value} when tag == :all ->
        [{stored_tag, key, value}]

      {{:"$goblin_tag", _tag, _key}, _seq, _value} when is_nil(tag) ->
        []

      {key, _seq, value} when is_nil(tag) or tag == :all ->
        [{key, value}]

      _ ->
        []
    end)
  end

  defp multi_search_mem_table(mem_table, keys, seq),
    do: MemTable.get_multi(mem_table, keys, seq)

  defp search_mem_table(mem_table, key, seq),
    do: MemTable.get(mem_table, key, seq)

  defp search_disk_tables(disk_tables, keys, seq) do
    DiskTables.search_iterators(disk_tables, keys, seq)
    |> Iterator.k_merge_stream()
    |> Enum.to_list()
  end

  defimpl Goblin.Tx do
    def put(_tx, _key, _value, _opts) do
      raise "Operation not allowed during read"
    end

    def put_multi(_tx, _pairs, _opts) do
      raise "Operation not allowed during read"
    end

    def remove(_tx, _key, _opts) do
      raise "Operation not allowed during read"
    end

    def remove_multi(_tx, _keys, _opts) do
      raise "Operation not allowed during read"
    end

    def get(tx, key, opts) do
      key =
        case opts[:tag] do
          nil -> key
          tag -> {:"$goblin_tag", tag, key}
        end

      case Broker.ReadTx.get(tx.mem_table, tx.disk_tables, tx.seq, key) do
        :not_found -> opts[:default]
        {_key, _seq, :"$goblin_tombstone"} -> opts[:default]
        {_key, _seq, value} -> value
      end
    end

    def get_multi(tx, keys, opts) do
      keys =
        case opts[:tag] do
          nil -> keys
          tag -> Enum.map(keys, &{:"$goblin_tag", tag, &1})
        end

      Broker.ReadTx.get_multi(tx.mem_table, tx.disk_tables, tx.seq, keys)
      |> Enum.flat_map(fn
        {_key, _seq, :"$goblin_tombstone"} -> []
        {{:"$goblin_tag", tag, key}, _seq, val} -> [{tag, key, val}]
        {key, _seq, val} -> [{key, val}]
      end)
      |> List.keysort(0)
    end

    def select(tx, opts) do
      min = opts[:min]
      max = opts[:max]
      tag = opts[:tag]

      [
        MemTable.iterator(tx.mem_table, tx.seq)
        | DiskTables.stream_iterators(tx.disk_tables, min, max, tx.seq)
      ]
      |> Broker.ReadTx.select(min, max, tag)
    end
  end
end
