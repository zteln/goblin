defmodule Talon.Tx do
  @moduledoc """

  """
  defdelegate put(tx, key, value), to: Talon.Writer.Transaction
  defdelegate remove(tx, key), to: Talon.Writer.Transaction
  defdelegate get(tx, key), to: Talon.Writer.Transaction
end
