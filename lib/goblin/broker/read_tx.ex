defmodule Goblin.Broker.ReadTx do
  @moduledoc false
  alias Goblin.{
    Broker,
    Broker.SnapshotRegistry,
    Iterator,
    Queryable
  }

  defstruct [
    :seq,
    :snapshot_ref,
    :snapshot_registry,
    :max_level_key
  ]

  @doc "Returns a new Tx struct for read-only transactions."
  @spec new(:ets.table(), reference(), Goblin.seq_no(), Goblin.level_key()) ::
          Goblin.Tx.t()
  def new(snapshot_registry, snapshot_ref, seq, max_level_key) do
    %__MODULE__{
      snapshot_registry: snapshot_registry,
      snapshot_ref: snapshot_ref,
      seq: seq,
      max_level_key: max_level_key
    }
  end

  @doc "Performs a search through tables retrieved via provided callback for multiple keys."
  @spec search(
          (Goblin.level_key() -> list(Queryable.t())),
          list(Goblin.db_key()),
          Goblin.seq_no(),
          Goblin.level_key()
        ) ::
          list(Goblin.pair())
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
      Iterator.k_merge_stream(fn ->
        Enum.map(tables_f.(level_key), &Queryable.search(&1, keys, seq))
      end)
      |> Enum.sort_by(fn {key, seq, _val} -> {key, -seq} end)
      |> Enum.uniq()
      |> Enum.reduce({acc, keys}, fn {key, _seq, val}, {acc, keys} ->
        {[{key, val} | acc], Enum.reject(keys, &(&1 == key))}
      end)

    search(tables_f, keys, seq, max_level_key, level_key + 1, acc)
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

      tables_f = fn level_key ->
        SnapshotRegistry.filter_tables(
          tx.snapshot_registry,
          tx.snapshot_ref,
          level_key: level_key,
          filter: &Queryable.has_key?(&1, key)
        )
      end

      case Broker.ReadTx.search(tables_f, [key], tx.seq, tx.max_level_key) do
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
        SnapshotRegistry.filter_tables(
          tx.snapshot_registry,
          tx.snapshot_ref,
          level_key: level_key,
          filter: fn table ->
            Enum.any?(keys, &Queryable.has_key?(table, &1))
          end
        )
      end

      Broker.ReadTx.search(tables_f, keys, tx.seq, tx.max_level_key)
    end
  end
end
