defmodule Goblin.Writer.Transaction do
  @moduledoc false

  defstruct [
    :fallback_read,
    :seq,
    writes: []
  ]

  @spec new(
          Goblin.seq_no(),
          Goblin.Writer.writer(),
          Goblin.Store.store(),
          (Goblin.db_key() -> term()) | nil
        ) :: Goblin.Tx.t()
  def new(seq, writer, store, fallback_read \\ nil) do
    fallback_read = fallback_read || (&Goblin.Reader.get(&1, writer, store))

    %__MODULE__{
      fallback_read: fallback_read,
      seq: seq
    }
  end

  @spec reverse_writes(Goblin.Tx.t()) :: Goblin.Tx.t()
  def reverse_writes(tx) do
    %{tx | writes: Enum.reverse(tx.writes)}
  end

  defimpl Goblin.Tx do
    def put(tx, key, value) do
      write = {:put, tx.seq, key, value}
      %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
    end

    def remove(tx, key) do
      write = {:remove, tx.seq, key}
      %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
    end

    def get(tx, key, default) do
      %{
        writes: writes,
        fallback_read: fallback_read
      } = tx

      tx_read =
        Enum.find(writes, fn
          {:put, _seq, ^key, _value} -> true
          {:remove, _seq, ^key} -> true
          _ -> false
        end)

      read = tx_read || fallback_read.(key)

      case read do
        :not_found -> default
        {_seq, value} -> value
        {:put, _seq, _key, value} -> value
        {:remove, _seq, _key} -> default
      end
    end
  end
end
