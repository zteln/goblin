defmodule SeaGoat.Store.Levels do
  @moduledoc """
  A hierarchical storage system that organizes entries into numbered levels.

  This module provides a data structure for storing and retrieving entries across
  multiple levels, where each level is identified by a non-negative integer. 
  Entries within each level are stored as lists, with new entries prepended to 
  maintain insertion order.

  ## Examples

      iex> levels = SeaGoat.Store.Levels.new()
      iex> levels = SeaGoat.Store.Levels.insert(levels, 0, "first entry")
      iex> levels = SeaGoat.Store.Levels.insert(levels, 0, "second entry")
      iex> levels = SeaGoat.Store.Levels.insert(levels, 1, "level 1 entry")
      iex> SeaGoat.Store.Levels.levels(levels)
      [0, 1]

  ## Data Structure

  The levels structure is implemented as a map where:
  - Keys are level numbers (non-negative integers)
  - Values are lists of entries for that level
  - New entries are prepended to existing lists
  """
  @type t :: map()
  @type level :: non_neg_integer()
  @type entry :: term()

  @doc """
  Creates a new, empty levels structure.

  ## Examples

      iex> SeaGoat.Store.Levels.new()
      %{}

  """
  @spec new() :: t()
  def new, do: %{}

  @doc """
  Inserts an entry into the specified level.

  If the level doesn't exist, it creates a new level with the entry as the only item.
  If the level exists, the entry is prepended to the existing entries list.

  ## Parameters

    - `levels` - The levels structure to insert into
    - `level` - The level number to insert the entry into
    - `entry` - The entry to insert (can be any term)

  ## Examples

      iex> levels = SeaGoat.Store.Levels.new()
      iex> levels = SeaGoat.Store.Levels.insert(levels, 0, "hello")
      iex> SeaGoat.Store.Levels.insert(levels, 0, "world")
      %{0 => ["world", "hello"]}

  """
  @spec insert(t(), level(), entry()) :: t()
  def insert(levels, level, entry) do
    Map.update(levels, level, [entry], &[entry | &1])
  end

  @doc """
  Removes entries from a level based on a predicate function.

  The predicate function is called for each entry in the specified level.
  If the predicate returns `true`, the entry is removed; if `false`, it remains.

  ## Parameters

    - `levels` - The levels structure to remove entries from
    - `level` - The level number to remove entries from
    - `predicate` - A function that takes an entry and returns a boolean

  ## Examples

      iex> levels = %{0 => [1, 2, 3, 4]}
      iex> SeaGoat.Store.Levels.remove(levels, 0, &(&1 > 2))
      %{0 => [1, 2]}

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

    case level_entries do
      [] ->
        Map.delete(levels, level)

      _ ->
        Map.put(levels, level, level_entries)
    end
  end

  @doc """
  Returns a sorted list of all level numbers that contain entries.

  ## Examples

      iex> levels = %{2 => ["a"], 0 => ["b"], 1 => ["c"]}
      iex> SeaGoat.Store.Levels.levels(levels)
      [0, 1, 2]

  """
  @spec levels(t()) :: [level()]
  def levels(levels) do
    levels
    |> Map.keys()
    |> Enum.sort()
  end

  @doc """
  Retrieves and optionally transforms entries from a level based on a predicate.

  First filters entries in the specified level using the predicate function.
  Then optionally transforms the filtered entries using the mapper function.

  ## Parameters

    - `levels` - The levels structure to query
    - `level` - The level number to retrieve entries from
    - `predicate` - A function that takes an entry and returns a boolean for filtering
    - `mapper` - A function to transform matching entries (defaults to identity function)

  ## Examples

      iex> levels = %{0 => [1, 2, 3, 4, 5]}
      iex> SeaGoat.Store.Levels.get_all_entries(levels, 0, &(&1 > 3))
      [4, 5]

      iex> SeaGoat.Store.Levels.get_all_entries(levels, 0, &(&1 > 3), &(&1 * 2))
      [8, 10]

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
