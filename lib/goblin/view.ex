defmodule Goblin.View do
  @moduledoc false

  @type t :: :ets.table()
  @type table :: Goblin.MemTable.t() | Goblin.DiskTable.t()
  @type level_key :: -1 | non_neg_integer()

  @spec new() :: t()
  def new() do
    :ets.new(:goblin_view, [
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  @spec update_sequence(t(), non_neg_integer()) :: :ok
  def update_sequence(ref, seq) do
    :ets.update_element(ref, :meta, {3, seq}, {:meta, -1, 0})
    :ok
  end

  @spec get_tables(t(), non_neg_integer(), level_key() | :_, (table() -> boolean())) ::
          list(table())
  def get_tables(ref, reader_epoch, lk \\ :_, filter \\ fn _ -> true end) do
    ms = [
      {
        {{:table, :"$1", :_}, :"$2", lk, :"$3"},
        [{:andalso, {:<, :"$1", reader_epoch}, {:<, reader_epoch, :"$2"}}],
        [:"$3"]
      }
    ]

    :ets.select(ref, ms)
    |> Enum.filter(&filter.(&1))
  end

  @spec add_table(t(), term(), level_key(), table()) :: :ok
  def add_table(ref, key, lk, table) do
    epoch = inc_get_epoch(ref)
    table_key = {:table, epoch, key}
    :ets.insert(ref, {table_key, nil, lk, table})

    case :ets.lookup(ref, :meta) do
      [] -> :ets.insert(ref, {:meta, lk, 0})
      [{:meta, max_lk, seq}] when lk > max_lk -> :ets.insert(ref, {:meta, lk, seq})
      _ -> nil
    end

    :ok
  end

  @spec soft_delete_table(t(), term()) :: :ok
  def soft_delete_table(ref, key) do
    dead = inc_get_epoch(ref)

    case :ets.match(ref, {{:table, :"$1", key}, :_, :_, :_}) do
      [] -> nil
      [[alive]] -> :ets.update_element(ref, {:table, alive, key}, {2, dead})
    end

    :ok
  end

  @spec acquire(t(), term()) :: {level_key(), non_neg_integer(), non_neg_integer()}
  def acquire(ref, key) do
    :ets.insert(ref, {{:reader, :pending, key}})

    {max_lk, seq} =
      case :ets.lookup(ref, :meta) do
        [] -> {-1, 0}
        [{:meta, max_lk, seq}] -> {max_lk, seq}
      end

    epoch = inc_get_epoch(ref)
    :ets.insert(ref, {{:reader, epoch, key}})
    :ets.delete(ref, {:reader, :pending, key})
    {max_lk, seq, epoch}
  end

  @spec release(t(), term()) :: :ok
  def release(ref, key) do
    :ets.match_delete(ref, {{:reader, :_, key}})
    :ok
  end

  @spec sweep(t()) :: list(table())
  def sweep(ref) do
    epoch = get_epoch(ref)
    min_epoch = find_min_reader_epoch(ref) || epoch

    case has_pending_reader?(ref) do
      true -> []
      false -> do_sweep(ref, min_epoch)
    end
  end

  defp find_min_reader_epoch(ref) do
    case :ets.next(ref, {:reader, -1, nil}) do
      {:reader, epoch, _key} when is_integer(epoch) -> epoch
      _ -> nil
    end
  end

  defp has_pending_reader?(ref), do: :ets.match(ref, {{:reader, :pending, :_}}) != []

  defp do_sweep(ref, epoch) do
    key = :ets.next(ref, {:table, -1, nil})
    do_sweep(ref, epoch, key, [])
  end

  defp do_sweep(_ref, _epoch, :"$end_of_table", acc), do: acc

  defp do_sweep(ref, epoch, key, acc) do
    case :ets.lookup(ref, key) do
      [{{:table, _, _}, dead, _, table}] when is_integer(dead) and dead <= epoch ->
        next_key = :ets.next(ref, key)
        :ets.delete(ref, key)
        do_sweep(ref, epoch, next_key, [table | acc])

      _ ->
        key = :ets.next(ref, key)
        do_sweep(ref, epoch, key, acc)
    end
  end

  defp inc_get_epoch(ref), do: :ets.update_counter(ref, :epoch, 1, {:epoch, 0})
  defp get_epoch(ref), do: :ets.lookup_element(ref, :epoch, 2, 0)
end
