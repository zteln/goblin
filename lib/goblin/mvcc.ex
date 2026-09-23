defmodule Goblin.MVCC do
  @moduledoc false

  alias Goblin.{MemTable, DiskTable}

  @type t :: :ets.table()
  @type table :: Goblin.MemTable.t() | Goblin.DiskTable.t()
  @type level_key :: -1 | non_neg_integer()

  @spec new() :: t()
  def new() do
    ref =
      :ets.new(:goblin_mvcc, [
        :public,
        :ordered_set,
        write_concurrency: true,
        read_concurrency: true
      ])

    :ets.insert(ref, {:meta, 0, 0, -1})
    ref
  end

  @spec update_sequence(t(), non_neg_integer()) :: :ok
  def update_sequence(ref, seq) do
    :ets.update_element(ref, :meta, {3, seq})
    :ok
  end

  @spec put_version(t(), list(), list()) :: :ok
  def put_version(ref, new, old) do
    [{:meta, version, _seq, max_lk}] = :ets.lookup(ref, :meta)

    version = version + 1
    max_lk = Enum.reduce(new, max_lk, &max(&1.level_key, &2))

    Enum.each(new, fn tab ->
      key = table_key(tab)
      :ets.insert(ref, {key, version, nil, tab})
    end)

    Enum.each(old, fn tab ->
      key = {:table, tab.level_key, tab.max_key, tab.min_key, tab.id}
      :ets.update_element(ref, key, {3, version})
    end)

    :ets.update_element(ref, :meta, [{2, version}, {4, max_lk}])
    :ok
  end

  @spec pin(t()) :: {reference(), non_neg_integer(), -1 | non_neg_integer()}
  def pin(ref) do
    key = make_ref()
    [{_, ver, seq, max_lk}] = :ets.lookup(ref, :meta)
    :ets.insert(ref, {{:pin, key}, ver, self()})

    case :ets.lookup_element(ref, :meta, 2) do
      ^ver ->
        {key, seq, max_lk}

      _ ->
        :ets.delete(ref, {:pin, key})
        pin(ref)
    end
  end

  @spec unpin(t(), reference() | pid()) :: :ok
  def unpin(ref, pid) when is_pid(pid) do
    :ets.match_delete(ref, {{:pin, :_}, :_, pid})
    :ok
  end

  def unpin(ref, key) do
    :ets.delete(ref, {:pin, key})
    :ok
  end

  @spec pinned?(t(), reference()) :: boolean()
  def pinned?(ref, key), do: :ets.member(ref, {:pin, key})

  @spec sweep(t()) :: list()
  def sweep(ref) do
    min_pinned =
      :ets.select(ref, [{{{:pin, :_}, :"$1", :_}, [], [:"$1"]}])
      |> Enum.min(fn -> :ets.lookup_element(ref, :meta, 2) end)

    :ets.select(ref, [
      {
        {{:table, :_, :_, :_, :_}, :_, :"$5", :"$6"},
        [
          {:andalso, {:"/=", :"$5", nil}, {:"=<", :"$5", min_pinned}}
        ],
        [:"$_"]
      }
    ])
    |> Enum.map(fn {key, _born, _dies, tab} ->
      :ets.delete(ref, key)
      tab
    end)
  end

  @spec get_all_tables(t(), reference()) :: {:ok, list()} | {:error, :no_pin}
  def get_all_tables(ref, pin_key) do
    with {:ok, version} <- pinned(ref, pin_key) do
      tabs =
        ref
        |> :ets.select([
          {
            {{:table, :_, :_, :_, :_}, :"$1", :"$2", :"$3"},
            [
              {:andalso, {:"=<", :"$1", version},
               {:orelse, {:==, :"$2", nil}, {:>, :"$2", version}}}
            ],
            [:"$3"]
          }
        ])
        |> List.flatten()

      {:ok, tabs}
    end
  end

  @spec get_matching_tables(t(), reference(), -1 | non_neg_integer(), list(term())) ::
          {:ok, list()} | {:error, :no_pin}
  def get_matching_tables(ref, pin_key, lk, _keys) when lk <= 0 do
    with {:ok, version} <- pinned(ref, pin_key) do
      tabs =
        ref
        |> :ets.select([
          {
            {{:table, lk, :_, :_, :_}, :"$1", :"$2", :"$3"},
            [
              {:andalso, {:"=<", :"$1", version},
               {:orelse, {:==, :"$2", nil}, {:>, :"$2", version}}}
            ],
            [:"$3"]
          }
        ])
        |> List.flatten()

      {:ok, tabs}
    end
  end

  def get_matching_tables(ref, pin_key, lk, [min_key | _] = keys) do
    with {:ok, version} <- pinned(ref, pin_key) do
      start = {:table, lk, min_key, min_key, ""}

      first =
        case :ets.prev(ref, start) do
          {:table, ^lk, _, _, _} = idx -> idx
          _ -> :ets.next(ref, start)
        end

      {:ok, walk(ref, version, lk, first, keys, [])}
    end
  end

  defp walk(_ref, _version, _lk, _idx, [], acc), do: acc

  defp walk(ref, version, lk, {:table, lk, max, min, _} = idx, keys, acc) do
    case visible(ref, version, idx) do
      nil ->
        walk(ref, version, lk, :ets.next(ref, idx), keys, acc)

      tab ->
        case Enum.drop_while(keys, &(&1 < min)) do
          [] ->
            acc

          [k | _] = keys when k <= max ->
            keys = Enum.drop_while(keys, &(&1 <= max))
            walk(ref, version, lk, :ets.next(ref, idx), keys, [tab | acc])

          keys ->
            walk(ref, version, lk, :ets.next(ref, idx), keys, acc)
        end
    end
  end

  defp walk(_ref, _version, _lk, _idx, _keys, acc), do: acc

  defp visible(ref, version, idx) do
    born = :ets.lookup_element(ref, idx, 2, nil)
    dies = :ets.lookup_element(ref, idx, 3, nil)

    with born when is_integer(born) and born <= version <- born,
         dies when is_nil(dies) or dies > version <- dies do
      :ets.lookup_element(ref, idx, 4, nil)
    else
      _ -> nil
    end
  end

  defp pinned(ref, pin_key) do
    case :ets.lookup_element(ref, {:pin, pin_key}, 2, nil) do
      nil -> {:error, :no_pin}
      version -> {:ok, version}
    end
  end

  defp table_key(%MemTable{} = mt), do: {:table, -1, nil, nil, mt.id}

  defp table_key(%DiskTable{key_range: {min, max}} = dt),
    do: {:table, dt.level_key, max, min, dt.id}
end
