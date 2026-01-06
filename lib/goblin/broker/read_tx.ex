defmodule Goblin.Broker.ReadTx do
  alias Goblin.MemTable
  alias Goblin.DiskTables
  alias Goblin.Iterator

  defstruct [
    :seq,
    :mem_table,
    :disk_tables
  ]

  @spec new(Goblin.table(), Goblin.table()) :: Goblin.Tx.t()
  def new(mem_table, disk_tables) do
    seq = MemTable.commit_seq(mem_table)
    %__MODULE__{seq: seq, mem_table: mem_table, disk_tables: disk_tables}
  end

  @spec get(Goblin.table(), Goblin.table(), Goblin.seq_no(), Goblin.db_key()) ::
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

  @spec get_multi(Goblin.table(), Goblin.table(), Goblin.seq_no(), [Goblin.db_key()]) :: [
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
    def put(_tx, _key, _value) do
      raise "Operation not allowed during read"
    end

    def put_multi(_tx, _pairs) do
      raise "Operation not allowed during read"
    end

    def remove(_tx, _key) do
      raise "Operation not allowed during read"
    end

    def remove_multi(_tx, _keys) do
      raise "Operation not allowed during read"
    end

    def get(tx, key, default) do
      case Goblin.Broker.ReadTx.get(tx.mem_table, tx.disk_tables, tx.seq, key) do
        :not_found -> default
        {_key, _seq, :"$goblin_tombstone"} -> default
        {_key, _seq, value} -> value
      end
    end

    def get_multi(tx, keys) do
      Goblin.Broker.ReadTx.get_multi(tx.mem_table, tx.disk_tables, tx.seq, keys)
      |> Enum.flat_map(fn
        {_k, _s, :"$goblin_tombstone"} -> []
        {k, _s, v} -> [{k, v}]
      end)
      |> List.keysort(0)
    end

    def select(tx, opts) do
      min = opts[:min]
      max = opts[:max]

      [
        MemTable.iterator(tx.mem_table, tx.seq)
        | DiskTables.stream_iterators(tx.disk_tables, min, max, tx.seq)
      ]
      |> Goblin.Iterator.k_merge_stream(min: min, max: max)
      |> Stream.map(fn {k, _s, v} -> {k, v} end)
    end
  end
end
