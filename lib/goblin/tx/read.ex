defmodule Goblin.Tx.Read do
  @moduledoc false

  alias Goblin.{
    Iterator,
    Broker,
    Queryable,
    Tx
  }

  defstruct [
    :name,
    :seq,
    :tx_id,
    :max_level_key
  ]

  @type t :: %__MODULE__{
          name: atom(),
          seq: non_neg_integer(),
          tx_id: term(),
          max_level_key: integer()
        }

  @spec new(atom(), term(), non_neg_integer(), integer()) :: t()
  def new(name, tx_id, seq, max_level_key) do
    %__MODULE__{
      name: name,
      tx_id: tx_id,
      seq: seq,
      max_level_key: max_level_key
    }
  end

  @spec search(
          (integer() -> [Goblin.Iterable.t()]),
          [term()],
          non_neg_integer(),
          integer(),
          integer(),
          [{term(), term()}]
        ) :: [{term(), term()}]
  def search(tables_f, keys, seq, max_level_key, level_key \\ -1, acc \\ [])

  def search(_tables_f, [], _seq, _max_level_key, _level, acc) do
    acc
    |> Enum.map(fn
      {{:"$goblin_tag", _tag, key}, val} -> {key, val}
      {key, val} -> {key, val}
    end)
    |> List.keysort(0)
  end

  def search(tables_f, _keys, seq, max_level_key, level, acc)
      when level > max_level_key,
      do: search(tables_f, [], seq, max_level_key, level, acc)

  def search(tables_f, keys, seq, max_level_key, level_key, acc) do
    {acc, keys} =
      Iterator.k_merge_stream(
        fn ->
          Enum.map(tables_f.(level_key), &Queryable.search(&1, keys, seq))
        end,
        filter_tombstones?: false
      )
      |> Enum.reduce({acc, MapSet.new(keys)}, fn
        {key, _seq, :"$goblin_tombstone"}, {acc, keys} ->
          {acc, MapSet.delete(keys, key)}

        {key, _seq, val}, {acc, keys} ->
          {[{key, val} | acc], MapSet.delete(keys, key)}
      end)

    search(tables_f, keys, seq, max_level_key, level_key + 1, acc)
  end

  defimpl Goblin.Transactionable do
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

      tables_f = fn level_key ->
        Broker.filter_tables(
          tx.name,
          tx.tx_id,
          level_key: level_key,
          filter: &Queryable.has_key?(&1, key)
        )
      end

      case Tx.Read.search(tables_f, [key], tx.seq, tx.max_level_key) do
        [] -> opts[:default]
        [{_key, value}] -> value
      end
    end

    def get_multi(tx, keys, opts) do
      keys =
        case opts[:tag] do
          nil -> keys
          tag -> Enum.map(keys, &{:"$goblin_tag", tag, &1})
        end

      tables_f = fn level_key ->
        Broker.filter_tables(
          tx.name,
          tx.tx_id,
          level_key: level_key,
          filter: fn table ->
            Enum.any?(keys, &Queryable.has_key?(table, &1))
          end
        )
      end

      Tx.Read.search(tables_f, keys, tx.seq, tx.max_level_key)
    end
  end
end
