defmodule Goblin.Snapshots do
  @moduledoc false

  @max_retries 60

  def new() do
    :ets.new(:snapshots, [
      :public,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  def add_table(ref, id, level_key, table, delete_func) do
    entry = %{table: table, delete: delete_func}
    :ets.insert(ref, {{:table, id}, true, 0, level_key, entry})
    :ok
  end

  def soft_delete_table(ref, id) do
    case :ets.match_object(ref, {{:table, id}, :_, :_, :_, :_}) do
      [] ->
        :ok

      [{key, _active?, _retries, _level_key, _entry}] ->
        :ets.update_element(ref, key, {2, false})
        :ok
    end
  end

  def hard_delete(ref) do
    :ets.match_object(ref, {{:table, :_}, false, :_, :_, :_})
    |> Enum.reduce_while(:ok, fn
      {_key, _active?, retries, _level_key, _entry}, _acc when retries > @max_retries ->
        {:halt, {:error, :too_many_retries}}

      {{:table, id} = key, _active?, _retries, _level_key, %{table: table, delete: delete}},
      acc ->
        case count_snapshots(ref, id) do
          0 ->
            delete.(table)
            :ets.delete(ref, key)

          _ ->
            :ets.update_counter(ref, key, {3, 1})
        end

        {:cont, acc}
    end)
  end

  def filter_tables(ref, tx_key, opts \\ []) do
    predicate = opts[:filter] || fn _ -> true end
    level_key = opts[:level_key] || :_

    :ets.match_object(ref, {{:snapshot, :_, tx_key}})
    |> Enum.flat_map(fn {{:snapshot, id, _ref}} ->
      case :ets.match_object(ref, {{:table, id}, :_, :_, level_key, :_}) do
        [{_key, _active?, _retries, _level_key, %{table: table}}] ->
          case predicate.(table) do
            true -> [table]
            false -> []
          end

        _ ->
          []
      end
    end)
  end

  def register_tx(ref, tx_key) do
    max_level_key =
      :ets.match_object(ref, {{:table, :_}, true, :_, :_, :_})
      |> Enum.reduce(
        -1,
        fn {{:table, id} = key, _active?, _retries, level_key, _entry}, acc ->
          snapshot_key = {:snapshot, id, tx_key}
          :ets.insert(ref, {snapshot_key})
          :ets.member(ref, key) || :ets.delete(ref, snapshot_key)

          if level_key > acc, do: level_key, else: acc
        end
      )

    seq =
      case :ets.lookup(ref, :seq) do
        [] -> 0
        [{:seq, seq}] -> seq
      end

    {max_level_key, seq}
  end

  def unregister_tx(ref, tx_key) do
    :ets.match_delete(ref, {{:snapshot, :_, tx_key}})
    :ok
  end

  def put_seq(ref, seq) do
    :ets.update_element(ref, :seq, {2, seq}, {:seq, 0})
  end

  defp count_snapshots(ref, id) do
    :ets.match_object(ref, {{:snapshot, id, :_}})
    |> Enum.count()
  end
end
