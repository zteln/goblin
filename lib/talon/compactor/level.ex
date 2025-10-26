defmodule Talon.Compactor.Level do
  @moduledoc false

  alias Talon.Compactor.Entry

  defstruct [
    :level_key,
    :compacting_ref,
    entries: %{}
  ]

  @type t :: %__MODULE__{}

  @spec get_total_size(t()) :: non_neg_integer()
  def get_total_size(level) do
    Enum.reduce(level.entries, 0, fn {_id, %{size: size}}, acc -> acc + size end)
  end

  @spec get_highest_prio_entries(t()) :: [Entry.t()]
  def get_highest_prio_entries(%{level_key: 0} = level) do
    level.entries
    |> Map.values()
  end

  def get_highest_prio_entries(level) do
    {_id, entry} =
      Enum.min_by(level.entries, fn {_id, entry} -> entry.priority end)

    [entry]
  end

  @spec put_entry(t(), Entry.t()) :: t()
  def put_entry(level, %Entry{} = entry) do
    entries = Map.put(level.entries, entry.id, entry)
    %{level | entries: entries}
  end

  @spec place_in_buffer(t(), {Talon.db_sequence(), Talon.db_key(), Talon.db_value()}) ::
          t()
  def place_in_buffer(level, {seq, key, value} = data) do
    case Map.keys(level.entries) do
      [] ->
        virtual_entry = %Entry{
          priority: 0,
          size: 0,
          key_range: {key, key},
          buffer: %{key => {seq, value}},
          is_virtual: true
        }

        put_entry(level, virtual_entry)

      _ ->
        sorted_entries =
          Enum.sort_by(level.entries, fn {_id, entry} -> elem(entry.key_range, 0) end)

        target_entry_id = find_target_entry(sorted_entries, key)

        entry = Map.get(level.entries, target_entry_id)
        updated_entry = Entry.place_in_buffer(entry, data)
        updated_entry = update_key_range(updated_entry, key)

        put_entry(level, updated_entry)
    end
  end

  defp find_target_entry(sorted_entries, key) do
    case Enum.find(sorted_entries, fn {_id, entry} -> Entry.in_range(entry, key) end) do
      {id, _entry} ->
        id

      nil ->
        case Enum.find(sorted_entries, fn {_id, entry} ->
               {_smallest, largest} = entry.key_range
               key <= largest
             end) do
          {id, _entry} ->
            id

          nil ->
            {id, _entry} = List.last(sorted_entries)
            id
        end
    end
  end

  defp update_key_range(entry, key) do
    {smallest, largest} = entry.key_range
    new_smallest = if smallest == nil or key < smallest, do: key, else: smallest
    new_largest = if largest == nil or key > largest, do: key, else: largest
    %{entry | key_range: {new_smallest, new_largest}}
  end
end
