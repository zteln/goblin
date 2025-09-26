defmodule SeaGoat.Store.Levels do
  @moduledoc """
  Contains all entries in separate levels.
  Provides functionality for inserting, updating, removing, and retrieving entries based on a level.
  """
  @type t :: map()
  @type level :: non_neg_integer()
  @type entry :: term()

  @doc """
  Returns a new levels structure.
  """
  @spec new() :: t()
  def new, do: %{}

  @doc """
  Insert `entry` into `level`, creating a new level if `level` does not exist, otherwise prepending `entry` to the existing level.
  """
  @spec insert(t(), level(), entry()) :: t()
  def insert(levels, level, entry) do
    Map.update(levels, level, [entry], &[entry | &1])
  end

  @doc """
  Remove entries in `level` according to `predicate`. 
  If `predicate` returns `true` for an entry, it is removed, otherwise it remains in the level.
  """
  @spec remove(t(), level(), (entry() -> boolean())) :: t()
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

  @doc """
  Returns a list of all levels.
  """
  @spec levels(t()) :: [level()]
  def levels(levels) do
    levels
    |> Map.keys()
    |> Enum.sort()
  end

  @doc """
  Get all entries in `level` that `predicate` returns `true` with.
  `mapper` transforms the entries that `predicate` filtered out.
  """
  @spec get_all_entries(t(), level(), (entry() -> boolean()), (entry() -> term())) ::
          entry() | term()
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
