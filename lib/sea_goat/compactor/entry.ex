defmodule SeaGoat.Compactor.Entry do
  defstruct [
    :id,
    :priority,
    :size,
    key_range: {nil, nil},
    buffer: [],
    is_virtual: false
  ]

  # def in_range(entry, key) do
  #   %{key_range: {smallest, largest}} = entry
  #   key >= smallest and key <= largest
  # end

  def in_range(entry, key) do
    case entry.key_range do
      {nil, nil} -> true
      {smallest, nil} -> key >= smallest
      {nil, largest} -> key <= largest
      {smallest, largest} -> key >= smallest and key <= largest
    end
  end

  def place_in_buffer(entry, data) do
    buffer = [data | entry.buffer]
    %{entry | buffer: buffer}
  end
end
