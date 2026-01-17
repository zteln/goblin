defmodule Goblin.MemTable.Store do
  @moduledoc false
  @type t :: :ets.table()

  @spec new(atom()) :: t()
  def new(name) do
    :ets.new(name, [:named_table, :ordered_set])
  end

  @spec insert_commit_seq(t(), Goblin.seq_no()) :: :ok
  def insert_commit_seq(store, seq) do
    :ets.insert(store, {:commit_seq, seq})
    :ok
  end

  @spec insert(t(), Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()) :: :ok
  def insert(store, key, seq, value) do
    :ets.insert(store, {{key, -seq}, value})
    :ok
  end

  @spec remove(t(), Goblin.db_key(), Goblin.seq_no()) :: :ok
  def remove(store, key, seq) do
    insert(store, key, seq, :"$goblin_tombstone")
    :ok
  end

  @spec get_commit_seq(t()) :: Goblin.seq_no()
  def get_commit_seq(store) do
    case :ets.lookup(store, :commit_seq) do
      [] -> 0
      [{_, seq}] -> seq
    end
  end

  @spec get(t(), Goblin.db_key(), Goblin.seq_no()) :: Goblin.triple() | :not_found
  def get(store, key, seq) do
    case :ets.lookup(store, {key, -seq}) do
      [] -> :not_found
      [{{key, seq}, value}] -> {key, abs(seq), value}
    end
  end

  @spec get_by_key(t(), Goblin.db_key(), Goblin.seq_no()) :: Goblin.triple() | :not_found
  def get_by_key(store, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:"=:=", :"$1", {:const, key}}, {:<, {:abs, :"$2"}, seq}}],
        [:"$_"]
      }
    ]

    :ets.select(store, ms)
    |> Enum.map(fn {{key, seq}, value} -> {key, abs(seq), value} end)
    |> Enum.max_by(fn {_key, seq, _value} -> seq end, fn -> :not_found end)
  end

  @spec delete_range(t(), Goblin.seq_no()) :: :ok
  def delete_range(store, seq) do
    ms = [{{{:_, :"$1"}, :_}, [{:<, {:abs, :"$1"}, seq}], [true]}]
    :ets.select_delete(store, ms)
    :ok
  end

  @spec iterate(t()) ::
          {Goblin.db_key(), Goblin.seq_no()} | :ready | :commit_seq | :end_of_iteration
  def iterate(store) do
    :ets.first(store)
    |> handle_iteration()
  end

  @spec iterate(t(), {Goblin.db_key(), Goblin.seq_no()} | :ready | :commit_seq) ::
          {Goblin.db_key(), Goblin.seq_no()} | :ready | :commit_seq | :end_of_iteration
  def iterate(store, {key, seq}) do
    :ets.next(store, {key, -seq})
    |> handle_iteration()
  end

  def iterate(store, next) do
    :ets.next(store, next)
    |> handle_iteration()
  end

  defp handle_iteration(:"$end_of_table"), do: :end_of_iteration
  defp handle_iteration({key, seq}), do: {key, abs(seq)}
  defp handle_iteration(next), do: next

  @spec set_ready(t()) :: :ok
  def set_ready(store) do
    :ets.insert(store, {:ready})
    :ok
  end

  @spec size(t()) :: non_neg_integer()
  def size(store) do
    :ets.info(store, :memory) * :erlang.system_info(:wordsize)
  end

  @spec wait_until_ready(t(), integer()) :: :ok
  def wait_until_ready(store, timeout \\ 5000)

  def wait_until_ready(_store, timeout) when timeout <= 0,
    do: raise("MemTable failed to get ready within timeout")

  def wait_until_ready(store, timeout) do
    if :ets.member(store, :ready) do
      :ok
    else
      Process.sleep(50)
      wait_until_ready(store, timeout - 50)
    end
  end
end
