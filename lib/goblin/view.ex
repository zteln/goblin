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

  def get_views(ref, filter) do
  end

  def add_view(ref, key, level_key, view) do
    epoch = inc_get_epoch(ref)
    view_key = {:view, epoch, key}
    :ets.insert(ref, {view_key, nil, level_key, view})

    case :ets.lookup(ref, :meta) do
      [] ->
        :ets.insert(ref, {:meta, level_key, 0})

      [{:meta, max_level_key, seq}] when level_key > max_level_key ->
        :ets.insert(ref, {:meta, level_key, seq})

      _ ->
        nil
    end

    :ok
  end

  def soft_delete_view(ref, key) do
    dead_epoch = inc_get_epoch(ref)

    case :ets.match(ref, {:view, key, :"$1"}) do
      [] ->
        :ok

      [[alive_epoch]] ->
        :ets.update_element(ref, {:view, key, alive_epoch}, {2, dead_epoch})
        :ok
    end
  end

  def register_reader(ref, key) do
    :ets.insert(ref, {{:reader, :pending, key}})
    epoch = get_epoch(ref)

    {max_lk, seq} =
      case :ets.lookup(ref, :meta) do
        [] -> {-1, 0}
        [{:meta, max_lk, seq}] -> {max_lk, seq}
      end

    :ets.insert(ref, {{:reader, epoch, key}})
    :ets.delete(ref, {{:reader, :pending, key}})
    {max_lk, seq, epoch}
  end

  def unregister_reader(ref, key) do
    :ets.match_delete(ref, {{:reader, :_, key}})
    :ok
  end

  def sweep(ref) do
    min_epoch = find_min_epoch(ref)
    # do_sweep(ref, min_reader_epoch)
  end

  defp find_min_epoch(ref) do
    epoch = inc_get_epoch(ref)

    case :ets.next(ref, {:reader, -1}) do
      {:reader, min_reader_epoch} -> min_reader_epoch
      _ -> epoch
    end
  end

  defp inc_get_epoch(ref), do: :ets.update_counter(ref, :epoch, 1, {:epoch, 0})
  defp get_epoch(ref), do: :ets.lookup_element(ref, :epoch, 1, 0)
end
