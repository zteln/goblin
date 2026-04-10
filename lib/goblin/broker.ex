defmodule Goblin.Broker do
  @moduledoc false

  @max_retries 60

  @spec new() :: :ets.table()
  def new() do
    :ets.new(:broker, [
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  @spec put_sequence(:ets.table(), non_neg_integer()) :: :ok
  def put_sequence(broker, sequence) do
    :ets.update_element(broker, :meta, {3, sequence}, {:meta, -1, 0})
    :ok
  end

  @spec add_table(
          :ets.table(),
          any(),
          -1 | non_neg_integer(),
          Goblin.Queryable.t(),
          (Goblin.Queryable.t() -> :ok)
        ) :: :ok
  def add_table(broker, id, level_key, table, delete_callback) do
    counter = inc_get_counter(broker)
    key = {counter, :table, id}
    soft_deleted_at = nil
    retries = {nil, 0}
    entry = %{table: table, delete: delete_callback}
    table_item = {key, soft_deleted_at, retries, level_key, entry}
    :ets.insert(broker, table_item)

    case :ets.lookup(broker, :meta) do
      [] ->
        :ets.insert(broker, {:meta, level_key, 0})
        :ok

      [{:meta, max_level_key, seq}] when level_key > max_level_key ->
        :ets.insert(broker, {:meta, level_key, seq})
        :ok

      _ ->
        :ok
    end
  end

  @spec soft_delete_table(:ets.table(), term()) :: :ok
  def soft_delete_table(broker, id) do
    soft_deleted_at = inc_get_counter(broker)

    case :ets.match(broker, {{:"$1", :table, id}, :_, :_, :_, :_}) do
      [] ->
        :ok

      [[insert_counter]] ->
        :ets.update_element(broker, {insert_counter, :table, id}, {2, soft_deleted_at})
        :ok
    end
  end

  @spec hard_delete_table(:ets.table(), term()) :: :ok | {:error, :too_many_retries}
  def hard_delete_table(broker, id) do
    case :ets.match_object(broker, {{:_, :table, id}, :_, :_, :_, :_}) do
      [{_, _, {_, retries}, _, _}] when retries > @max_retries ->
        {:error, :too_many_retries}

      [table_item] ->
        try_hard_delete(broker, table_item)
    end
  end

  @spec hard_delete_tables(:ets.table()) :: :ok
  def hard_delete_tables(broker) do
    ms = soft_deleted_tables_ms()

    :ets.select(broker, ms)
    |> Enum.each(&try_hard_delete(broker, &1))
  end

  @spec filter_tables(:ets.table(), non_neg_integer(), keyword()) :: list(Goblin.Queryable.t())
  def filter_tables(broker, tx_counter, opts \\ []) do
    predicate = opts[:filter] || fn _ -> true end
    level_key = opts[:level_key] || :_
    ms = visible_tables_ms(level_key, tx_counter)

    :ets.select(broker, ms)
    |> Enum.filter(&predicate.(&1))
  end

  @spec register_tx(:ets.table(), reference()) ::
          {-1 | non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def register_tx(broker, tx_key) do
    :ets.insert(broker, {{:pending_tx, :tx, tx_key}})

    {max_level_key, seq} =
      case :ets.lookup(broker, :meta) do
        [] -> {-1, 0}
        [{:meta, max_level_key, seq}] -> {max_level_key, seq}
      end

    counter = inc_get_counter(broker)
    :ets.insert(broker, {{counter, :tx, tx_key}})
    :ets.delete(broker, {:pending_tx, :tx, tx_key})

    {max_level_key, seq, counter}
  end

  @spec unregister_tx(:ets.table(), reference()) :: :ok
  def unregister_tx(broker, tx_key) do
    :ets.match_delete(broker, {{:_, :tx, tx_key}})
    :ok
  end

  defp try_hard_delete(broker, table_item) do
    {
      {insert_counter, _, id} = key,
      soft_deleted_at,
      {_, retries},
      _,
      entry
    } = table_item

    case table_free?(broker, insert_counter, soft_deleted_at) do
      true ->
        %{table: table, delete: delete} = entry
        :ets.delete(broker, key)
        delete.(table)

      false ->
        timer_ref = Process.send_after(self(), {:retry_hard_delete, id}, 1000)
        :ets.update_element(broker, key, {3, {timer_ref, retries + 1}})
    end

    :ok
  end

  defp table_free?(broker, insert_counter, soft_delete_counter) do
    ms = count_active_tx_in_range_ms(insert_counter, soft_delete_counter)
    :ets.select_count(broker, ms) == 0
  end

  defp inc_get_counter(broker) do
    :ets.update_counter(broker, :counter, 1, {:counter, 0})
  end

  defp count_active_tx_in_range_ms(insert_counter, soft_delete_counter) do
    [
      {
        {{:"$1", :tx, :_}},
        [
          {
            :orelse,
            {:andalso, {:<, insert_counter, :"$1"}, {:<, :"$1", soft_delete_counter}},
            {:==, :"$1", :pending_tx}
          }
        ],
        [true]
      }
    ]
  end

  defp visible_tables_ms(level_key, counter) do
    [
      {
        {{:"$1", :table, :_}, :"$2", :_, level_key, %{table: :"$3"}},
        [{:andalso, {:is_integer, :"$1"}, {:<, :"$1", counter}, {:<, counter, :"$2"}}],
        [:"$3"]
      }
    ]
  end

  defp soft_deleted_tables_ms() do
    [
      {
        {{:_, :table, :_}, :"$2", {:"$3", :_}, :_, :_},
        [{:andalso, {:is_integer, :"$2"}, {:==, :"$3", nil}}],
        [:"$_"]
      }
    ]
  end
end
