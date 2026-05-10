defmodule Goblin.View do
  @moduledoc false

  def new() do
    :ets.new(:goblin_view, [
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  def get_views(ref, reader_epoch, filter) do
  end

  def update_sequence(ref, seq),
    do: :ets.update_element(ref, :meta, {3, seq}, {:meta, -1, 0})

  def add_view(ref, key, lk, view) do
    epoch = inc_get_epoch(ref)
    view_key = {:view, epoch, key}
    :ets.insert(ref, {view_key, nil, lk, view})

    case :ets.lookup(ref, :meta) do
      [] -> :ets.insert(ref, {:meta, lk, 0})
      [{:meta, max_lk, seq}] when lk > max_lk -> :ets.insert(ref, {:meta, lk, seq})
      _ -> nil
    end

    :ok
  end

  def soft_delete_view(ref, key) do
    dead_epoch = inc_get_epoch(ref)

    case :ets.match(ref, {{:view, :"$1", key}, :_, :_, :_}) do
      [] ->
        :ok

      [[alive_epoch]] ->
        :ets.update_element(ref, {:view, alive_epoch, key}, {2, dead_epoch})
        :ok
    end
  end

  def acquire(ref, key) do
    :ets.insert(ref, {{:reader, :pending, key}})

    {max_lk, seq} =
      case :ets.lookup(ref, :meta) do
        [] -> {-1, 0}
        [{:meta, max_lk, seq}] -> {max_lk, seq}
      end

    epoch = inc_get_epoch(ref)
    :ets.insert(ref, {{:reader, epoch, key}})
    :ets.delete(ref, {{:reader, :pending, key}})
    {max_lk, seq, epoch}
  end

  def release(ref, key) do
    :ets.match_delete(ref, {{:reader, :_, key}})
    :ok
  end

  def sweep(ref) do
    epoch = inc_get_epoch(ref)
    min_epoch = find_min_reader_epoch(ref) || epoch - 1

    case has_pending_reader?(ref) do
      false -> do_sweep(ref, min_epoch)
      true -> :ok
    end
  end

  defp find_min_reader_epoch(ref) do
    case :ets.next(ref, {:reader, -1, nil}) do
      {:reader, :pending, _key} -> find_min_reader_epoch(ref)
      {:reader, min_reader_epoch, _key} -> min_reader_epoch
      _ -> nil
    end
  end

  defp has_pending_reader?(ref), do: :ets.match(ref, {{:reader, :pending, :_}}) != []

  defp do_sweep(ref, epoch) do
    key = :ets.next(ref, {:view, -1, nil})
    do_sweep(ref, epoch, key)
  end

  defp do_sweep(_ref, _epoch, :"$end_of_table"), do: :ok

  defp do_sweep(ref, epoch, key) do
    case :ets.lookup(ref, key) do
      {{:view, _, _}, nil, _, _} ->
        key = :ets.next(ref, key)
        do_sweep(ref, epoch, key)

      {{:view, _, _}, dead, _, table} when dead <= epoch ->
        key = :ets.next(ref, key)
        # remove table
        do_sweep(ref, epoch, key)

      {{:view, _, _}, dead, _, _} when dead > epoch ->
        :ok

      _ ->
        key = :ets.next(ref, key)
        do_sweep(ref, epoch, key)
    end
  end

  defp inc_get_epoch(ref), do: :ets.update_counter(ref, :epoch, 1, {:epoch, 0})
  # defp get_epoch(ref), do: :ets.lookup_element(ref, :epoch, 2, 0)
end
