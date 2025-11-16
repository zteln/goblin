defmodule Goblin.Reader.Transaction do
  @moduledoc false

  defstruct [
    :seq,
    :writer,
    :store
  ]

  @spec new(Goblin.seq_no(), Goblin.Writer.writer(), Goblin.Store.store()) :: Goblin.Tx.t()
  def new(seq, writer, store) do
    %__MODULE__{
      seq: seq,
      writer: writer,
      store: store
    }
  end

  defimpl Goblin.Tx do
    def put(_tx, _key, _value) do
      raise "Not allowed to write during a read-only transaction"
    end

    def remove(_tx, _key) do
      raise "Not allowed to write during a read-only transaction"
    end

    def get(tx, key, default) do
      %{
        seq: seq,
        writer: writer,
        store: store
      } = tx

      case Goblin.Reader.get(key, writer, store, seq) do
        :not_found -> default
        {_seq, value} -> value
      end
    end
  end
end
