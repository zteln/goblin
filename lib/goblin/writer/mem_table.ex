defmodule Goblin.Writer.MemTable do
  @moduledoc false
  @type t :: :ets.table()

  @spec new(atom()) :: t()
  def new(name) do
    :ets.new(name, [:named_table, :ordered_set])
  end

  @spec size(t()) :: non_neg_integer()
  def size(table) do
    :ets.info(table, :size) - 2
  end

  @spec put_commit_seq(t(), Goblin.seq_no()) :: true
  def put_commit_seq(_table, seq) when seq < 0 do
    true
  end

  def put_commit_seq(table, seq) do
    :ets.insert(table, {:commit_seq, seq})
  end

  @spec set_ready(t()) :: true
  def set_ready(table) do
    :ets.insert(table, {:ready})
  end

  @spec upsert(t(), Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()) :: true
  def upsert(table, key, seq, value) do
    :ets.insert(table, {{key, seq}, value})
  end

  @spec delete(t(), Goblin.seq_no(), Goblin.db_key()) :: true
  def delete(table, key, seq) do
    :ets.insert(table, {{key, seq}, :"$goblin_tombstone"})
  end

  @spec read(t(), Goblin.db_key(), Goblin.seq_no() | nil) ::
          {Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()} | :not_found
  def read(table, key, nil) do
    wait_until_memtable_ready(table)
    commit_seq = commit_seq(table)

    case do_read(table, key, commit_seq) do
      :not_found -> :not_found
      {{key, seq}, value} -> {key, seq, value}
    end
  end

  def read(table, key, seq) do
    wait_until_memtable_ready(table)

    case do_read(table, key, seq) do
      :not_found -> :not_found
      {{key, seq}, value} -> {key, seq, value}
    end
  end

  @spec get_range(t(), Goblin.db_key(), Goblin.db_key()) :: [Goblin.triple()]
  def get_range(table, min, max) do
    wait_until_memtable_ready(table)

    commit_seq = commit_seq(table)

    guard =
      cond do
        is_nil(min) and is_nil(max) -> [{:"=<", :"$2", commit_seq}]
        is_nil(min) -> [{:and, {:"=<", :"$1", max}, {:"=<", :"$2", commit_seq}}]
        is_nil(max) -> [{:and, {:"=<", min, :"$1"}, {:"=<", :"$2", commit_seq}}]
        true -> [{:and, {:"=<", :"$1", max}, {:"=<", min, :"$1"}, {:"=<", :"$2", commit_seq}}]
      end

    ms = [{{{:"$1", :"$2"}, :_}, guard, [:"$_"]}]

    :ets.select(table, ms)
    |> Enum.map(fn
      {{key, seq}, value} -> {key, seq, value}
    end)
  end

  @spec clean_seq_range(t(), Goblin.seq_no()) :: non_neg_integer()
  def clean_seq_range(table, seq) do
    ms = [{{{:_, :"$1"}, :_}, [{:"=<", :"$1", seq}], [true]}]
    :ets.select_delete(table, ms)
  end

  @spec get_seq_range(t(), Goblin.seq_no()) :: [Goblin.triple()]
  def get_seq_range(table, seq) do
    ms = [{{{:_, :"$1"}, :_}, [{:"=<", :"$1", seq}], [:"$_"]}]

    :ets.select(table, ms)
    |> Enum.map(fn {{key, seq}, value} -> {key, seq, value} end)
  end

  @spec commit_seq(t()) :: Goblin.seq_no() | -1
  def commit_seq(table) do
    case :ets.lookup(table, :commit_seq) do
      [] -> -1
      [{_, commit_seq}] -> commit_seq
    end
  end

  defp wait_until_memtable_ready(table, timeout \\ 5000)
  defp wait_until_memtable_ready(_table, 0), do: raise("MemTable failed to get ready")

  defp wait_until_memtable_ready(table, timeout) do
    if :ets.member(table, :ready) do
      :ok
    else
      Process.sleep(50)
      wait_until_memtable_ready(table, timeout - 50)
    end
  end

  defp do_read(table, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:"=:=", :"$1", key}, {:"=<", :"$2", seq}}],
        [:"$_"]
      }
    ]

    :ets.select(table, ms)
    |> Enum.max_by(fn {{_key, seq}, _value} -> seq end, fn -> :not_found end)
  end
end
