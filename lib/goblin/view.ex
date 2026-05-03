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

  def add_view(ref, id, level_key, view) do
    epoch = epoch(ref)
    view_key = {epoch, :view, id, nil}
    :ets.insert(ref, {view_key, level_key, view})

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

  # def soft_delete_view(ref, id) do
  #   epoch = epoch(ref)
  #
  #   # case :ets.match(ref, {{:view, id}, :"$1", :_, :_})
  # end

  def sweep(ref) do
  end

  def register_reader(ref, key) do
  end

  def unregister_reader(ref, key) do
  end

  defp epoch(ref), do: :ets.update_counter(ref, :epoch, 1, {:epoch, 0})
end
