defmodule Goblin.Broker.WriteTx do
  @moduledoc false
  alias Goblin.{
    Broker,
    Broker.SnapshotRegistry,
    Queryable
  }

  defstruct [
    :seq,
    :snapshot_ref,
    :snapshot_registry,
    :max_level_key,
    writes: []
  ]

  @doc "Returns a new Tx struct that can both read and write."
  @spec new(:ets.table(), reference(), Goblin.seq_no(), Goblin.level_key()) :: Goblin.Tx.t()
  def new(snapshot_registry, snapshot_ref, seq, max_level_key) do
    %__MODULE__{
      snapshot_registry: snapshot_registry,
      snapshot_ref: snapshot_ref,
      seq: seq,
      max_level_key: max_level_key
    }
  end

  defimpl Goblin.Tx do
    def put(tx, key, value, opts) do
      key =
        case opts[:tag] do
          nil -> key
          tag -> {:"$goblin_tag", tag, key}
        end

      write = {:put, tx.seq, key, value}
      %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
    end

    def put_multi(tx, pairs, opts) do
      tagger =
        case opts[:tag] do
          nil -> fn key -> key end
          tag -> fn key -> {:"$goblin_tag", tag, key} end
        end

      Enum.reduce(pairs, tx, fn {key, value}, acc ->
        write = {:put, acc.seq, tagger.(key), value}
        %{acc | seq: acc.seq + 1, writes: [write | acc.writes]}
      end)
    end

    def remove(tx, key, opts) do
      key =
        case opts[:tag] do
          nil -> key
          tag -> {:"$goblin_tag", tag, key}
        end

      write = {:remove, tx.seq, key}
      %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
    end

    def remove_multi(tx, keys, opts) do
      tagger =
        case opts[:tag] do
          nil -> fn key -> key end
          tag -> fn key -> {:"$goblin_tag", tag, key} end
        end

      Enum.reduce(keys, tx, fn key, acc ->
        write = {:remove, acc.seq, tagger.(key)}
        %{acc | seq: acc.seq + 1, writes: [write | acc.writes]}
      end)
    end

    def get(tx, key, opts) do
      key =
        case opts[:tag] do
          nil -> key
          tag -> {:"$goblin_tag", tag, key}
        end

      tx_table =
        tx.writes
        |> Enum.map(fn
          {:put, seq, key, value} -> {key, seq, value}
          {:remove, seq, key} -> {key, seq, :"$goblin_tombstone"}
        end)
        |> Enum.sort_by(fn {key, seq, _value} -> {key, -seq} end)

      tables_f = fn level_key ->
        [
          tx_table
          | SnapshotRegistry.filter_tables(
              tx.snapshot_registry,
              tx.snapshot_ref,
              level_key: level_key,
              filter: &Queryable.has_key?(&1, key)
            )
        ]
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

      tx_table =
        tx.writes
        |> Enum.map(fn
          {:put, seq, key, value} -> {key, seq, value}
          {:remove, seq, key} -> {key, seq, :"$goblin_tombstone"}
        end)
        |> Enum.sort_by(fn {key, seq, _value} -> {key, -seq} end)

      tables_f = fn level_key ->
        [
          tx_table
          | SnapshotRegistry.filter_tables(
              tx.snapshot_registry,
              tx.snapshot_ref,
              level_key: level_key,
              filter: fn table ->
                Enum.any?(keys, &Queryable.has_key?(table, &1))
              end
            )
        ]
      end

      Broker.ReadTx.search(tables_f, keys, tx.seq, tx.max_level_key)
      |> List.keysort(0)
    end
  end
end
