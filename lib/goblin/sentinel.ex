defmodule Goblin.Sentinel do
  @moduledoc false

  defmacro tombstone, do: :"$goblin_tombstone"
  defmacro unset, do: :"$goblin_nil"
  defmacro tagged, do: :"$goblin_tag"

  defguard is_tombstone(v) when v == :"$goblin_tombstone"
  defguard is_set(v) when v != :"$goblin_nil"
end
