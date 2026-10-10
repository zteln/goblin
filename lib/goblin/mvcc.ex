defmodule Goblin.MVCC do
  @moduledoc false

  alias Goblin.MemTable
  alias Goblin.DiskTable

  @type t :: :ets.table()
  @typep table :: Goblin.MemTable.t() | Goblin.DiskTable.t()
  @typep level_key :: -1 | non_neg_integer()

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
  def update_sequence(ref, sqn) do
    :ets.update_element(ref, :meta, {3, sqn})
    :ok
  end

  @spec put_version(t(), list(table()), list(table())) :: :ok
  def put_version(ref, new, old) do
    [{:meta, version, _sqn, max_lk}] = :ets.lookup(ref, :meta)
    version = version + 1
    max_lk = Enum.reduce(new, max_lk, &max(&1.level_key, &2))

    Enum.each(new, fn tab ->
      key = table_key(tab)
      :ets.insert(ref, {key, version, nil, tab})
    end)

    Enum.each(old, fn tab ->
      key = table_key(tab)
      :ets.update_element(ref, key, {3, version})
    end)

    :ets.update_element(ref, :meta, [{2, version}, {4, max_lk}])
    :ok
  end

  @spec pin(t(), reference()) :: {non_neg_integer(), level_key()}
  def pin(ref, key) do
    [{_, ver, sqn, max_lk}] = :ets.lookup(ref, :meta)
    :ets.insert(ref, {{:pin, key}, ver, self()})

    case :ets.lookup_element(ref, :meta, 2) do
      ^ver ->
        {sqn, max_lk}

      _ ->
        :ets.delete(ref, {:pin, key})
        pin(ref, key)
    end
  end

  @spec unpin(t(), reference()) :: :ok
  def unpin(ref, key) do
    :ets.delete(ref, {:pin, key})
    :ok
  end

  @spec pinned?(t(), reference()) :: boolean()
  def pinned?(ref, key), do: :ets.member(ref, {:pin, key})

  @spec sweep(t()) :: list(table())
  def sweep(ref) do
    current = :ets.lookup_element(ref, :meta, 2)

    min_pinned =
      :ets.select(ref, [{{{:pin, :_}, :_, :_}, [], [:"$_"]}])
      |> Enum.reduce(current, fn {key, ver, pid}, acc ->
        if Process.alive?(pid) do
          min(ver, acc)
        else
          :ets.delete(ref, key)
          acc
        end
      end)

    :ets.select(ref, [
      {
        {{:table, :_, :_, :_, :_}, :_, :"$5", :_},
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

  @spec get_all_tables(t(), reference()) :: list(table())
  def get_all_tables(ref, pin_key), do: get_matching_tables(ref, pin_key, :_, [])

  @spec get_matching_tables(t(), reference(), level_key(), list(term())) ::
          list(table())
  def get_matching_tables(ref, pin_key, lk, keys) do
    ver = pinned(ref, pin_key)

    range_guards =
      if is_integer(lk) and lk > 0,
        do: [{:>=, :"$1", {:const, List.first(keys)}}, {:"=<", :"$2", {:const, List.last(keys)}}],
        else: []

    ref
    |> :ets.select([
      {
        {{:table, lk, :"$1", :"$2", :_}, :"$3", :"$4", :"$5"},
        [
          {:andalso, {:"=<", :"$3", ver}, {:orelse, {:==, :"$4", nil}, {:>, :"$4", ver}}}
          | range_guards
        ],
        [:"$5"]
      }
    ])
  end

  defp pinned(ref, pin_key) do
    :ets.lookup_element(ref, {:pin, pin_key}, 2, nil) ||
      raise ArgumentError,
            "transaction is no longer active (used outside its Goblin.read/transaction scope?)"
  end

  defp table_key(%MemTable{} = mt), do: {:table, mt.level_key, nil, nil, mt.ref}

  defp table_key(%DiskTable{key_range: {min, max}} = dt),
    do: {:table, dt.level_key, max, min, dt.id}
end
