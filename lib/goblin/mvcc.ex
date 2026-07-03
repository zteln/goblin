defmodule Goblin.MVCC do
  @moduledoc false

  @type t :: :ets.table()
  @type table :: Goblin.MemTable.t() | Goblin.DiskTable.t()
  @type level_key :: -1 | non_neg_integer()

  @spec new() :: t()
  def new() do
    :ets.new(:goblin_mvcc, [
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  @spec put_snapshot(t(), map(), non_neg_integer()) :: :ok
  def put_snapshot(ref, levels, seq) do
    version =
      case current_meta(ref) do
        :empty -> 0
        {_, _, version} -> version + 1
      end

    max_lk =
      Enum.reduce(levels, -1, fn
        {lk, level}, max_lk when lk <= 0 ->
          Enum.each(level, fn t ->
            :ets.insert(ref, {{:table, version, lk, nil, nil, t.id}, t})
          end)

          max(lk, max_lk)

        {lk, level}, max_lk ->
          Enum.each(level, fn %{key_range: {min, max}} = dt ->
            :ets.insert(ref, {{:table, version, lk, max, min, dt.id}, dt})
          end)

          max(lk, max_lk)
      end)

    :ets.insert(ref, {{:snapshot, version}, seq, max_lk})
    :ok
  end

  @spec get_tables(t(), non_neg_integer()) :: list(table())
  def get_tables(ref, version) do
    :ets.match(ref, {{:table, version, :_, :_, :_, :_}, :"$1"})
    |> List.flatten()
  end

  def get_tables(ref, version, lk, _keys) when lk <= 0 do
    next = :ets.next(ref, {:table, version, lk, nil, nil, ""})
    enumerate(ref, version, lk, next, [])
  end

  def get_tables(ref, version, lk, [min_key | _] = keys) do
    start = {:table, version, lk, min_key, min_key, ""}

    acc =
      case :ets.prev(ref, start) do
        {:table, ^version, ^lk, ^min_key, _min, _id} = idx -> [:ets.lookup_element(ref, idx, 2)]
        _ -> []
      end

    merge(ref, version, lk, keys, :ets.next(ref, start), acc)
  end

  @spec add_reader(t(), term()) :: {non_neg_integer(), level_key(), non_neg_integer()}
  def add_reader(ref, reader_key) do
    :ets.insert(ref, {{:reader, :pending, reader_key}})

    case current_meta(ref) do
      {seq, max_lk, version} ->
        :ets.insert(ref, {{:reader, version, reader_key}})
        :ets.delete(ref, {:reader, :pending, reader_key})
        {seq, max_lk, version}

      :empty ->
        :ets.delete(ref, {:reader, :pending, reader_key})
        raise "MVCC.add_reader called before any snapshots were published"
    end
  end

  @spec release_reader(t(), term()) :: :ok
  def release_reader(ref, reader_key) do
    :ets.match_delete(ref, {{:reader, :_, reader_key}})
    :ok
  end

  @spec sweep(t()) :: list(table())
  def sweep(ref) do
    max_v =
      case current_meta(ref) do
        :empty -> 0
        {_, _, max_v} -> max_v
      end

    first_v =
      case :ets.next(ref, {:snapshot, -1}) do
        {:snapshot, v} -> v
        _ -> max_v
      end

    case has_pending_reader?(ref) do
      true -> []
      false -> sweepable_tables(ref, first_v, max_v)
    end
  end

  defp sweepable_tables(ref, v, max_v, acc \\ {MapSet.new(), MapSet.new()})

  defp sweepable_tables(ref, v, max_v, {all, in_use}) when v >= max_v do
    in_use =
      get_tables(ref, max_v)
      |> MapSet.new()
      |> MapSet.union(in_use)

    MapSet.difference(all, in_use)
    |> MapSet.to_list()
  end

  defp sweepable_tables(ref, v, max_v, {all, in_use}) do
    key = {:snapshot, v}

    tables =
      get_tables(ref, v)
      |> MapSet.new()

    {all, in_use} =
      case in_use?(ref, v) do
        true ->
          in_use = MapSet.union(in_use, tables)
          all = MapSet.union(all, tables)
          {all, in_use}

        false ->
          all = MapSet.union(all, tables)
          :ets.match_delete(ref, {{:table, v, :_, :_, :_, :_}, :_})
          :ets.delete(ref, key)
          {all, in_use}
      end

    case :ets.next(ref, key) do
      {:snapshot, next_v} -> sweepable_tables(ref, next_v, max_v, {all, in_use})
      _ -> sweepable_tables(ref, max_v, max_v, {all, in_use})
    end
  end

  defp enumerate(ref, version, lk, {:table, version, lk, _max, _min, _id} = idx, acc) do
    enumerate(ref, version, lk, :ets.next(ref, idx), [:ets.lookup_element(ref, idx, 2) | acc])
  end

  defp enumerate(_ref, _version, _lk, _idx, acc), do: acc

  defp merge(_ref, _version, _lk, [], _idx, acc), do: acc

  defp merge(ref, version, lk, keys, {:table, version, lk, max_key, min_key, _id} = idx, acc) do
    case Enum.drop_while(keys, &(&1 < min_key)) do
      [] ->
        acc

      [k | _] = keys when k <= max_key ->
        keys = Enum.drop_while(keys, &(&1 <= max_key))

        merge(ref, version, lk, keys, :ets.next(ref, idx), [
          :ets.lookup_element(ref, idx, 2) | acc
        ])

      keys ->
        merge(ref, version, lk, keys, :ets.next(ref, idx), acc)
    end
  end

  defp merge(_ref, _version, _lk, _keys, _idx, acc), do: acc

  defp current_meta(ref) do
    case :ets.prev(ref, {:snapshot, nil}) do
      {:snapshot, version} = key ->
        seq = :ets.lookup_element(ref, key, 2)
        max_lk = :ets.lookup_element(ref, key, 3)
        {seq, max_lk, version}

      _ ->
        :empty
    end
  end

  defp in_use?(ref, v), do: :ets.match(ref, {{:reader, v, :_}}) != []
  defp has_pending_reader?(ref), do: :ets.match(ref, {{:reader, :pending, :_}}) != []
end
