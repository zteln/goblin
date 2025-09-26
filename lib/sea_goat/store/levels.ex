defmodule SeaGoat.Store.Levels do
  @moduledoc """

  """
  @type t :: map()
  @type level :: non_neg_integer()
  @type entry :: term()

  @doc """

  """
  @spec new() :: t()
  def new, do: %{}

  @spec insert(t(), level(), entry()) :: t()
  def insert(levels, level, entry) do
    Map.update(levels, level, [entry], &[entry | &1])
  end

  def update(levels, level, updater) do
    level_entries =
      levels
      |> Map.get(level, [])
      |> Enum.map(&updater.(&1))

    Map.put(levels, level, level_entries)
  end

  def remove(levels, level, predicate) do
    level_entries =
      levels
      |> Map.get(level, [])
      |> Enum.flat_map(fn entry ->
        if predicate.(entry) do
          []
        else
          [entry]
        end
      end)

    Map.put(levels, level, level_entries)
  end

  def levels(levels) do
    levels
    |> Map.keys()
    |> Enum.sort()
  end

  def get_entry(levels, level, getter) do
    levels
    |> Map.get(level, [])
    |> Enum.find(&getter.(&1))
  end

  def get_all_entries(levels, level, predicate, mapper \\ & &1) do
    levels
    |> Map.get(level, [])
    |> Enum.flat_map(fn entry ->
      if predicate.(entry) do
        [mapper.(entry)]
      else
        []
      end
    end)
  end
end
