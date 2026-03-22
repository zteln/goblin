defmodule Goblin.Snapshots do
  @moduledoc false

  @max_retries 60

  @spec new() :: :ets.table()
  def new() do
    :ets.new(:snapshots, [
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  @spec add_table(
          :ets.table(),
          term(),
          -1 | non_neg_integer(),
          Goblin.Queryable.t(),
          (Goblin.Queryable.t() -> :ok)
        ) :: :ok
  def add_table(ref, id, level_key, table, delete_func) do
    counter = :ets.update_counter(ref, :counter, 1, {:counter, 0})
    entry = %{table: table, delete: delete_func}
    :ets.insert(ref, {{counter, :table, id}, false, {nil, 0}, level_key, entry})
    :ok
  end

  @spec soft_delete_table(:ets.table(), term()) :: :ok
  def soft_delete_table(ref, id) do
    counter = :ets.update_counter(ref, :counter, 1, {:counter, 0})

    case :ets.match(ref, {{:"$1", :table, id}, :_, :_, :_, :_}) do
      [] ->
        :ok

      [[insert_counter]] ->
        :ets.update_element(ref, {insert_counter, :table, id}, {2, counter})
        :ok
    end
  end

  @spec hard_delete_table(:ets.table(), term()) :: :ok | {:error, :too_many_retries}
  def hard_delete_table(ref, id) do
    case :ets.match_object(ref, {{:_, :table, id}, :_, :_, :_, :_}) do
      [{_, _, {_, retries}, _, _}] when retries > @max_retries ->
        {:error, :too_many_retries}

      [obj] ->
        try_hard_delete(ref, obj)
    end
  end

  @spec hard_delete_tables(:ets.table()) :: :ok
  def hard_delete_tables(ref) do
    ms = [
      {
        {{:_, :table, :_}, :"$2", {:"$3", :_}, :_, :_},
        [{:andalso, {:is_integer, :"$2"}, {:==, :"$3", nil}}],
        [:"$_"]
      }
    ]

    :ets.select(ref, ms)
    |> Enum.each(&try_hard_delete(ref, &1))
  end

  @spec filter_tables(:ets.table(), reference(), keyword()) :: list(Goblin.Queryable.t())
  def filter_tables(ref, tx_key, opts \\ []) do
    predicate = opts[:filter] || fn _ -> true end
    level_key = opts[:level_key] || :_
    [[tx_counter]] = :ets.match(ref, {{:"$1", :tx, tx_key}})

    ms = [
      {
        {{:"$1", :table, :_}, :"$2", :_, level_key, %{table: :"$3"}},
        [{:andalso, {:is_integer, :"$1"}, {:<, :"$1", tx_counter}, {:<, tx_counter, :"$2"}}],
        [:"$3"]
      }
    ]

    :ets.select(ref, ms)
    |> Enum.filter(&predicate.(&1))
  end

  @spec register_tx(:ets.table(), reference()) :: {-1 | non_neg_integer(), non_neg_integer()}
  def register_tx(ref, tx_key) do
    :ets.insert(ref, {{:pending_tx, :tx, tx_key}})

    seq =
      case :ets.lookup(ref, :seq) do
        [] -> 0
        [{:seq, seq}] -> seq
      end

    counter = :ets.update_counter(ref, :counter, 1, {:counter, 0})
    :ets.insert(ref, {{counter, :tx, tx_key}})
    :ets.delete(ref, {:pending_tx, :tx, tx_key})

    ms = [
      {
        {{:"$1", :table, :_}, :"$2", :_, :"$3", :_},
        [{:andalso, {:<, :"$1", counter}, {:<, counter, :"$2"}}],
        [:"$3"]
      }
    ]

    max_level_key =
      :ets.select(ref, ms)
      |> Enum.max(fn -> -1 end)

    {max_level_key, seq}
  end

  @spec unregister_tx(:ets.table(), reference()) :: :ok
  def unregister_tx(ref, tx_key) do
    :ets.match_delete(ref, {{:_, :tx, tx_key}})
    :ok
  end

  @spec put_seq(:ets.table(), non_neg_integer()) :: :ok
  def put_seq(ref, seq) do
    :ets.update_element(ref, :seq, {2, seq}, {:seq, 0})
    :ok
  end

  defp try_hard_delete(
         ref,
         {{insert_counter, _, id} = key, soft_delete_counter, {_, retries}, _, entry}
       ) do
    case check_table_usage(ref, insert_counter, soft_delete_counter) do
      :ok ->
        %{table: table, delete: delete} = entry
        :ets.delete(ref, key)
        delete.(table)

      :in_use ->
        timer_ref = Process.send_after(self(), {:retry_hard_delete, id}, 1000)
        :ets.update_element(ref, key, {3, {timer_ref, retries + 1}})
    end

    :ok
  end

  defp check_table_usage(ref, insert_counter, soft_delete_counter) do
    ms = [
      {
        {{:"$1", :tx, :_}},
        [
          {
            :orelse,
            {:andalso, {:<, insert_counter, :"$1"}, {:<, :"$1", soft_delete_counter}},
            {:==, :"$1", :pending_tx}
          }
        ],
        [:"$_"]
      }
    ]

    case :ets.select(ref, ms) do
      [] -> :ok
      _ -> :in_use
    end
  end
end
