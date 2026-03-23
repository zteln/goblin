defmodule Goblin.TestHelper do
  @moduledoc false

  def uniq_by_value(list, f \\ & &1) do
    Enum.reduce(list, [], fn item, acc ->
      case Enum.any?(acc, &(f.(&1) == f.(item))) do
        true -> acc
        false -> [item | acc]
      end
    end)
    |> Enum.reverse()
  end
end

ExUnit.start(exclude: :property_tests)
