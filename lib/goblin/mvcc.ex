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

  @spec put_snapshot(t(), list(Goblin.MemTable.t()), map(), non_neg_integer()) :: :ok
  def put_snapshot(ref, mem_tables, levels, seq) do
    version =
      case current_meta(ref) do
        :empty -> -1
        {_, _, version} -> version
      end

    max_lk = levels |> Map.keys() |> Enum.max(fn -> -1 end)
    :ets.insert(ref, {{:snapshot, version + 1}, seq, max_lk, mem_tables, levels})
    :ok
  end

  @spec get_tables(t(), non_neg_integer()) :: list(Goblin.MemTable.t() | Goblin.DiskTable.t())
  @spec get_tables(t(), non_neg_integer(), level_key()) ::
          list(Goblin.MemTable.t() | Goblin.DiskTable.t())
  def get_tables(ref, version) do
    case :ets.match(ref, {{:snapshot, version}, :_, :_, :"$1", :"$2"}) do
      [[mem_tables, levels]] -> mem_tables ++ (Map.values(levels) |> List.flatten())
      _ -> []
    end
  end

  def get_tables(ref, version, -1) do
    case :ets.match(ref, {{:snapshot, version}, :_, :_, :"$1", :_}) do
      [[mem_tables]] -> mem_tables
      _ -> []
    end
  end

  def get_tables(ref, version, lk) do
    case :ets.match(ref, {{:snapshot, version}, :_, :_, :_, %{lk => :"$1"}}) do
      [[level]] -> level
      _ -> []
    end
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

  @spec sweep(t()) :: list(Goblin.MemTable.t() | Goblin.DiskTable.t())
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
          :ets.delete(ref, key)
          {all, in_use}
      end

    case :ets.next(ref, key) do
      {:snapshot, next_v} -> sweepable_tables(ref, next_v, max_v, {all, in_use})
      _ -> sweepable_tables(ref, max_v, max_v, {all, in_use})
    end
  end

  defp in_use?(ref, v), do: :ets.match(ref, {{:reader, v, :_}}) != []

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

  defp has_pending_reader?(ref), do: :ets.match(ref, {{:reader, :pending, :_}}) != []
end
