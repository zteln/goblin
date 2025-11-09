defmodule Goblin.Writer.Transaction do
  @moduledoc false

  defstruct [
    :fallback_read,
    :seq,
    writes: []
  ]

  @type t :: %__MODULE__{}

  @spec new(Goblin.seq_no(), (Goblin.db_key() -> term())) :: t()
  def new(seq, fallback_read \\ fn _ -> :not_found end) do
    %__MODULE__{
      fallback_read: fallback_read,
      seq: seq
    }
  end

  @spec put(t(), Goblin.db_key(), Goblin.db_value()) :: t()
  def put(tx, key, value) do
    write = {:put, tx.seq, key, value}
    %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
  end

  @spec remove(t(), Goblin.db_key()) :: t()
  def remove(tx, key) do
    write = {:remove, tx.seq, key}
    %{tx | seq: tx.seq + 1, writes: [write | tx.writes]}
  end

  @spec get(t(), Goblin.db_key(), term()) :: Goblin.db_value()
  def get(tx, key, default \\ nil) do
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
      {:value, _seq, value} -> value
      {:put, _seq, _key, value} -> value
      {:remove, _seq, _key} -> default
    end
  end
end
