defmodule Goblin.Writer.MemTable do
  @moduledoc false
  @type t :: :ets.table()

  @spec new(atom()) :: t()
  def new(name) do
    :ets.new(name, [:named_table, :ordered_set])
  end

  @spec size(t()) :: non_neg_integer()
  def size(table) do
    :ets.info(table, :size)
  end

  @spec put_commit_seq(t(), Goblin.seq_no()) :: true
  def put_commit_seq(table, seq) do
    :ets.insert(table, {:commit_seq, seq})
  end

  @spec set_ready(t()) :: true
  def set_ready(table) do
    :ets.insert(table, {:ready})
  end

  @spec upsert(t(), Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()) :: true
  def upsert(table, key, seq, value) do
    :ets.insert(table, {key, seq, value})
  end

  @spec delete(t(), Goblin.seq_no(), Goblin.db_key()) :: true
  def delete(table, key, seq) do
    :ets.insert(table, {key, seq, :tombstone})
  end

  @spec read(t(), Goblin.db_key()) ::
          {Goblin.db_key(), Goblin.seq_no(), Goblin.db_value()} | :not_found
  def read(table, key) do
    wait_until_memtable_ready(table)

    commit_seq = commit_seq(table)

    :ets.select(table, [
      {
        {:"$1", :"$2", :_},
        [{:andalso, {:"=:=", :"$1", key}, {:"=<", :"$2", commit_seq}}],
        [:"$_"]
      }
    ])
    |> Enum.max_by(&elem(&1, 1), fn -> :not_found end)
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

    ms = [{{:"$1", :"$2", :_}, guard, [:"$_"]}]
    :ets.select(table, ms)
  end

  @spec clean_seq_range(t(), Goblin.seq_no()) :: non_neg_integer()
  def clean_seq_range(table, seq) do
    :ets.select_delete(table, seq_range_ms(seq, true))
  end

  @spec get_seq_range(t(), Goblin.seq_no()) :: [Goblin.triple()]
  def get_seq_range(table, seq) do
    :ets.select(table, seq_range_ms(seq))
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

  defp commit_seq(table) do
    case :ets.lookup(table, :commit_seq) do
      [] -> -1
      [{_, commit_seq}] -> commit_seq
    end
  end

  defp seq_range_ms(seq, match \\ :"$_") do
    [
      {
        {:_, :"$1", :_},
        [{:"=<", :"$1", seq}],
        [match]
      }
    ]
  end
end
