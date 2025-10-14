defmodule SeaGoat.Compactor.Level do
  alias SeaGoat.Compactor.Entry

  defstruct [
    :level_key,
    :compacting_ref,
    entries: %{}
  ]

  def get_total_size(level) do
    Enum.reduce(level.entries, 0, fn {_id, %{size: size}}, acc -> acc + size end)
  end

  def get_highest_prio_entry(level) do
    {_id, entry} =
      Enum.min_by(level.entries, fn {_id, entry} -> entry.priority end)

    entry
  end

  def place_in_buffer(level, {_seq, key, _value} = data) do
    case Map.keys(level.entries) do
      [] ->
        # No entries exist, create a virtual entry
        virtual_entry = %Entry{
          priority: 0,
          size: 0,
          key_range: {key, key},
          buffer: [data],
          is_virtual: true
        }

        put_entry(level, virtual_entry)

      _ ->
        # Find the correct entry based on key ranges
        sorted_entries =
          Enum.sort_by(level.entries, fn {_id, entry} -> elem(entry.key_range, 0) end)

        target_entry_id = find_target_entry(sorted_entries, key)

        # Update the target entry with the new data
        entry = Map.get(level.entries, target_entry_id)
        updated_entry = Entry.place_in_buffer(entry, data)
        updated_entry = update_key_range(updated_entry, key)

        # entries = Map.put(level.entries, target_entry_id, updated_entry)
        # %{level | entries: entries}
        put_entry(level, updated_entry)
    end
  end

  defp find_target_entry(sorted_entries, key) do
    case Enum.find(sorted_entries, fn {_id, entry} -> Entry.in_range(entry, key) end) do
      {id, _entry} ->
        id

      nil ->
        # Key doesn't fit in any existing range, find the closest entry
        case Enum.find(sorted_entries, fn {_id, entry} ->
               {_smallest, largest} = entry.key_range
               key <= largest
             end) do
          {id, _entry} ->
            id

          nil ->
            # Key is larger than all existing ranges, use the last entry
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

  def put_entry(level, entry) do
    entries = Map.put(level.entries, entry.id, entry)
    %{level | entries: entries}
  end

  #
  # def place_in_buffer(level, {_seq, key, _value} = data) do
  # end

  # def place_in_buffer(level, key, data) do
  #   # Place into correct buffer
  #   # key1 < key2 < key3 < inf
  #   sorted_key_range =
  #     level.entries
  #     |> Enum.map(fn {id, %{key_range: {smallest, _largest}}} -> {id, smallest} end)
  #     |> List.keysort(1)
  #
  #   foo =
  #     sorted_key_range
  #     |> Enum.take_while(fn {_id, smallest} -> smallest < key end)
  #
  #   cond do
  #     sorted_key_range == [] and foo == [] ->
  #       # Empty level
  #       # Fill up levels tmp_buffer
  #       buffer = [data | level.buffer]
  #       %{level | buffer: buffer}
  #
  #     foo == [] ->
  #       # Smaller than smallest key, place in first entry
  #       {id, _} = List.first(sorted_key_range)
  #
  #       entry =
  #         level.entries
  #         |> Map.get(id)
  #         |> Map.update(:buffer, [data], &[data | &1])
  #
  #       entries = Map.put(level.entries, id, entry)
  #       %{level | entries: entries}
  #
  #     true ->
  #       # Take the last element in foo and place in that buffer
  #       {id, _} = List.last(sorted_key_range)
  #
  #       entry =
  #         level.entries
  #         |> Map.get(id)
  #         |> Map.update(:buffer, [data], &[data | &1])
  #
  #       entries = Map.put(level.entries, id, entry)
  #       %{level | entries: entries}
  #   end
  # end
end
