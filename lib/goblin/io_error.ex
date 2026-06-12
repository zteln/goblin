defmodule Goblin.IOError do
  @moduledoc """
  Raised when Goblin cannot read or write its underlying storage files.

  The exception has the following fields:
  - `:operation` which describes which operation failed
  - `:path` which states which file it failed to operate on
  - `:reason` which describes why the operation failed
  """
  defexception [:operation, :path, :reason]

  @impl true
  def message(%{operation: op, path: path, reason: reason}) do
    "#{op} failed on #{path}: #{format_reason(reason)}"
  end

  defp format_reason(reason) when is_atom(reason), do: :file.format_error(reason)
  defp format_reason(reason), do: inspect(reason)
end
