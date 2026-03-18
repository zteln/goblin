defmodule Goblin.Tx.Write do
  @moduledoc false

  alias Goblin.{
    Snapshots,
    Queryable,
    Tx
  }

  defstruct [
    :seq,
    :tx_key,
    :name,
    :max_level_key,
    writes: []
  ]

  @type t :: %__MODULE__{
          seq: non_neg_integer(),
          tx_key: term(),
          name: atom(),
          max_level_key: integer(),
          writes: list()
        }

  @doc "Creates a new write transaction."
  @spec new(atom(), term(), non_neg_integer(), integer()) :: t()
  def new(name, tx_key, seq, max_level_key) do
    %__MODULE__{
      name: name,
      tx_key: tx_key,
      seq: seq,
      max_level_key: max_level_key
    }
  end

  @doc "Finalizes the transaction by reversing the accumulated writes."
  @spec complete(t()) :: t()
  def complete(tx) do
    %{tx | writes: Enum.reverse(tx.writes)}
  end

  defimpl Goblin.Transactionable do
    def put(_tx, nil, _value, _opts),
      do: raise(ArgumentError, "not allowed to write with key `nil`")

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

      Enum.reduce(pairs, tx, fn
        {nil, _value}, _acc ->
          raise ArgumentError, "not allowed to write with key `nil`"

        {key, value}, acc ->
          write = {:put, acc.seq, tagger.(key), value}
          %{acc | seq: acc.seq + 1, writes: [write | acc.writes]}
      end)
    end

    def remove(_tx, nil, _opts), do: raise(ArgumentError, "not allowed to write with key `nil`")

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

      Enum.reduce(keys, tx, fn
        nil, _acc ->
          raise ArgumentError, "not allowed to write with key `nil`"

        key, acc ->
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
          | Snapshots.filter_tables(
              tx.name,
              tx.tx_key,
              level_key: level_key,
              filter: &Queryable.has_key?(&1, key)
            )
        ]
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
          | Snapshots.filter_tables(
              tx.name,
              tx.tx_key,
              level_key: level_key,
              filter: fn table ->
                Enum.any?(keys, &Queryable.has_key?(table, &1))
              end
            )
        ]
      end

      Tx.Read.search(tables_f, keys, tx.seq, tx.max_level_key)
      |> List.keysort(0)
    end
  end
end
