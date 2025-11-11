defmodule Goblin.Compactor.Entry do
  @moduledoc false

  defstruct [
    :id,
    :priority,
    :size,
    key_range: {nil, nil},
    buffer: %{},
    is_virtual: false
  ]

  @type t :: %__MODULE__{}

  @spec in_range(t(), Goblin.db_key()) :: boolean()
  def in_range(entry, key) do
    case entry.key_range do
      {nil, nil} -> true
      {smallest, nil} -> key >= smallest
      {nil, largest} -> key <= largest
      {smallest, largest} -> key >= smallest and key <= largest
    end
  end

  @spec place_in_buffer(t(), {Goblin.db_sequence(), Goblin.db_key(), Goblin.db_value()}) ::
          t()
  def place_in_buffer(entry, {key, seq, value}) do
    buffer =
      Map.merge(entry.buffer, %{key => {seq, value}}, fn _k, {s1, v1}, {s2, v2} ->
        cond do
          s1 > s2 -> {s1, v1}
          s2 > s1 -> {s2, v2}
        end
      end)

    %{entry | buffer: buffer}
  end
end
