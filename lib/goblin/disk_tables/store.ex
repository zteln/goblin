defmodule Goblin.DiskTables.Store do
  @moduledoc false
  alias Goblin.BloomFilter

  @type t :: :ets.table()

  @spec new(atom()) :: t()
  def new(name) do
    :ets.new(name, [:named_table])
  end

  @spec set_ready(t()) :: :ok
  def set_ready(store) do
    :ets.insert(store, {:ready})
    :ok
  end

  @spec insert(t(), Goblin.DiskTables.DiskTable.t()) :: :ok
  def insert(store, disk_table) do
    :ets.insert(store, {disk_table.file, disk_table.key_range, disk_table})
    :ok
  end

  @spec remove(t(), Path.t()) :: :ok
  def remove(store, key) do
    :ets.delete(store, key)
    :ok
  end

  @spec select_within_key_range(t(), Goblin.db_key()) :: [Path.t()]
  def select_within_key_range(store, key) do
    key = if is_tuple(key), do: {:const, key}, else: key

    guard = [
      {:andalso, {:"=<", :"$1", key}, {:"=<", key, :"$2"}}
    ]

    ms = [{{:_, {:"$1", :"$2"}, :"$3"}, guard, [:"$3"]}]

    :ets.select(store, ms)
    |> Enum.filter(fn disk_table ->
      BloomFilter.member?(disk_table.bloom_filter, key)
    end)
  end

  @spec select_within_bounds(t(), Goblin.db_key() | nil, Goblin.db_key() | nil) :: [
          Goblin.DiskTables.DiskTable.t()
        ]
  def select_within_bounds(store, min, max) do
    guard =
      cond do
        is_nil(min) and is_nil(max) -> []
        is_nil(min) -> [{:"=<", :"$2", max}]
        is_nil(max) -> [{:"=<", min, :"$3"}]
        true -> [{:andalso, {:"=<", :"$2", max}, {:"=<", min, :"$3"}}]
      end

    ms = [{{:"$1", {:"$2", :"$3"}, :_}, guard, [:"$1"]}]

    :ets.select(store, ms)
  end

  @spec wait_until_ready(t(), integer()) :: :ok
  def wait_until_ready(store, timeout \\ 5000)

  def wait_until_ready(_store, timeout) when timeout <= 0,
    do: raise("DiskTables failed to get ready within timeout")

  def wait_until_ready(store, timeout) do
    if :ets.member(store, :ready) do
      :ok
    else
      Process.sleep(50)
      wait_until_ready(store, timeout - 50)
    end
  end
end
