defprotocol Goblin.Brokerable do
  @moduledoc false
  @type t :: t()

  @spec id(t()) :: any()
  def id(table)

  @spec level_key(t()) :: -1 | non_neg_integer()
  def level_key(table)

  @spec remove(t()) :: :ok
  def remove(table)
end
