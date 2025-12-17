defmodule Goblin.Writer.MemTable do
  @moduledoc false
  @type t :: :ets.table()

  defmodule Iterator do
    @moduledoc false
    defstruct [
      :idx,
      :table,
      :max_seq,
      :min_key,
      :max_key
    ]

    defimpl Goblin.Iterable do
      def init(iterator), do: iterator

      def next(%{idx: nil} = iterator) do
        idx = :ets.first(iterator.table)
        handle_iteration(iterator, idx)
      end

      def next(iterator) do
        idx = :ets.next(iterator.table, iterator.idx)
        handle_iteration(iterator, idx)
      end

      def close(_iterator), do: :ok

      defp handle_iteration(_iterator, :"$end_of_table"), do: :ok

      defp handle_iteration(
             %{min_key: nil, max_key: nil, max_seq: max_seq} = iterator,
             {key, seq} = idx
           )
           when seq < max_seq do
        [{{_key, _seq}, value}] = :ets.lookup(iterator.table, idx)
        {{key, seq, value}, %{iterator | idx: idx}}
      end

      defp handle_iteration(
             %{min_key: nil, max_key: max_key, max_seq: max_seq} = iterator,
             {key, seq} = idx
           )
           when key <= max_key and seq < max_seq do
        [{{_key, _seq}, value}] = :ets.lookup(iterator.table, idx)
        {{key, seq, value}, %{iterator | idx: idx}}
      end

      defp handle_iteration(
             %{min_key: min_key, max_key: nil, max_seq: max_seq} = iterator,
             {key, seq} = idx
           )
           when min_key <= key and seq < max_seq do
        [{{_key, _seq}, value}] = :ets.lookup(iterator.table, idx)
        {{key, seq, value}, %{iterator | idx: idx}}
      end

      defp handle_iteration(%{max_seq: max_seq} = iterator, {key, seq} = idx)
           when seq < max_seq do
        [{{_key, _seq}, value}] = :ets.lookup(iterator.table, idx)
        {{key, seq, value}, %{iterator | idx: idx}}
      end

      defp handle_iteration(iterator, idx) do
        next(%{iterator | idx: idx})
      end
    end
  end

  @spec new(atom()) :: t()
  def new(name) do
    :ets.new(name, [:named_table, :ordered_set])
  end

  @spec size(t()) :: non_neg_integer()
  def size(table) do
    :ets.info(table, :memory) * :erlang.system_info(:wordsize)
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

    case read_key(table, key, commit_seq) do
      :not_found -> :not_found
      {{key, seq}, value} -> {key, seq, value}
    end
  end

  def read(table, key, seq) do
    wait_until_memtable_ready(table)

    case read_key(table, key, seq) do
      :not_found -> :not_found
      {{key, seq}, value} -> {key, seq, value}
    end
  end

  @spec iterator(t(), keyword()) :: Goblin.Iterable.t()
  def iterator(table, opts \\ []) do
    wait_until_memtable_ready(table)
    max_seq = opts[:max_seq] || commit_seq(table)

    %Iterator{
      table: table,
      min_key: opts[:min_key],
      max_key: opts[:max_key],
      max_seq: max_seq
    }
  end

  @spec clean_seq_range(t(), Goblin.seq_no()) :: non_neg_integer()
  def clean_seq_range(table, seq) do
    ms = [{{{:_, :"$1"}, :_}, [{:"=<", :"$1", seq}], [true]}]
    :ets.select_delete(table, ms)
  end

  @spec commit_seq(t()) :: Goblin.seq_no()
  def commit_seq(table) do
    case :ets.lookup(table, :commit_seq) do
      [] -> 0
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

  defp read_key(table, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:"=:=", :"$1", key}, {:<, :"$2", seq}}],
        [:"$_"]
      }
    ]

    :ets.select(table, ms)
    |> Enum.max_by(fn {{_key, seq}, _value} -> seq end, fn -> :not_found end)
  end
end
