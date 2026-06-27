defmodule Goblin.DiskTable.MemIndex do
  @moduledoc false

  def new(name) do
    :ets.new(name, [
      :ordered_set,
      :public,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  def insert_index(_ref, _id, []), do: :ok

  def insert_index(ref, id, [{key, pos} | index]) do
    :ets.insert(ref, {{id, key}, pos})
    insert_index(ref, id, index)
  end

  def lookup(ref, id, key) do
    case :ets.prev(ref, {id, key}) do
      {^id, _} = idx -> :ets.lookup_element(ref, idx, 2)
    end
  end
end
