defmodule Goblin.MemTables.MemTable do
  @moduledoc false

  defstruct [
    :wal_name,
    :ref
  ]

  @type t :: %__MODULE__{}

  @doc "Returns a new MemTable instance."
  @spec new(Path.t()) :: t()
  def new(wal_name) do
    ref = :ets.new(:mem_table, [:ordered_set])
    %__MODULE__{wal_name: wal_name, ref: ref}
  end

  @doc "Deletes the provided MemTable."
  @spec delete(t()) :: :ok
  def delete(mem_table) do
    :ets.delete(mem_table.ref)
    :ok
  end

  @doc "Give away the MemTable to `pid`."
  @spec give_away(t(), pid()) :: :ok
  def give_away(mem_table, to) do
    :ets.give_away(mem_table.ref, to, :mem_table)
    :ok
  end

  @doc "Insert the given `key`, `value` and `seq` into the MemTable."
  @spec insert(t(), Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()) :: :ok
  def insert(mem_table, key, seq, value) do
    :ets.insert(mem_table.ref, {{key, -seq}, value})
    :ok
  end

  @doc "Removes a key in the MemTable by appending a tombstone associated with the key."
  @spec remove(t(), Goblin.db_key(), Goblin.seq_no()) :: :ok
  def remove(mem_table, key, seq) do
    insert(mem_table, key, seq, :"$goblin_tombstone")
  end

  @doc "Looks up a key in the MemTable with the specific sequence number."
  @spec get(t(), Goblin.db_key(), Goblin.seq_no()) :: Goblin.triple() | :not_found
  def get(mem_table, key, seq) do
    case :ets.lookup(mem_table.ref, {key, -seq}) do
      [] -> :not_found
      [{{key, seq}, value}] -> {key, abs(seq), value}
    end
  end

  @doc "Looks up a matching key that has a sequence number strictly lower than the provided sequence."
  @spec get_by_key(t(), Goblin.db_key(), Goblin.seq_no()) :: Goblin.triple() | :not_found
  def get_by_key(mem_table, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:==, :"$1", {:const, key}}, {:<, {:abs, :"$2"}, seq}}],
        [:"$_"]
      }
    ]

    :ets.select(mem_table.ref, ms)
    |> Enum.map(fn {{key, seq}, value} -> {key, abs(seq), value} end)
    |> Enum.max_by(fn {_key, seq, _value} -> seq end, fn -> :not_found end)
  end

  @doc "Returns a boolean indicating whether the provided key exists in the MemTable or not."
  @spec has_key?(t(), Goblin.db_key()) :: boolean()
  def has_key?(mem_table, key) do
    ms = [
      {
        {{:"$1", :_}, :_},
        [{:==, :"$1", {:const, key}}],
        [:"$_"]
      }
    ]

    case :ets.select(mem_table.ref, ms) do
      [] -> false
      _ -> true
    end
  end

  @doc "Start an iteration over the MemTable."
  @spec iterate(t()) :: {Goblin.db_key(), Goblin.seq_no()} | :end_of_iteration
  def iterate(mem_table) do
    idx = :ets.first(mem_table.ref)
    handle_iteration(mem_table, idx)
  end

  @doc "Continue iterating through the MemTable."
  @spec iterate(t(), {Goblin.db_key(), Goblin.seq_no()}) ::
          {Goblin.db_key(), Goblin.seq_no()} | :end_of_iteration
  def iterate(mem_table, {key, seq}) do
    idx = :ets.next(mem_table.ref, {key, -seq})
    handle_iteration(mem_table, idx)
  end

  def iterate(mem_table, idx) do
    idx = :ets.next(mem_table.ref, idx)
    handle_iteration(mem_table, idx)
  end

  defp handle_iteration(_mem_table, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_mem_table, {key, seq}), do: {key, abs(seq)}
  defp handle_iteration(mem_table, idx), do: iterate(mem_table, idx)

  @doc "Return the current size of the MemTable."
  @spec size(t()) :: non_neg_integer()
  def size(mem_table) do
    :ets.info(mem_table.ref, :memory) * :erlang.system_info(:wordsize)
  end
end

defimpl Goblin.Queryable, for: Goblin.MemTables.MemTable do
  alias Goblin.MemTables.{MemTable, Iterator}

  def has_key?(mem_table, key) do
    MemTable.has_key?(mem_table, key)
  end

  def search(mem_table, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case MemTable.get_by_key(mem_table, key, seq) do
        {_key, _seq, _value} = triple -> [triple]
        _ -> []
      end
    end)
  end

  def stream(mem_table, _min, _max, seq) do
    %Iterator{mem_table: mem_table, max_seq: seq}
  end
end
