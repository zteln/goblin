defmodule SeaGoatDB.Tx do
  @moduledoc """

  """
  defdelegate put(tx, key, value), to: SeaGoatDB.Writer.Transaction
  defdelegate remove(tx, key), to: SeaGoatDB.Writer.Transaction
  defdelegate get(tx, key), to: SeaGoatDB.Writer.Transaction
end
