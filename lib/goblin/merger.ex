defmodule Goblin.Merger do
  @moduledoc false

  defstruct levels: %{},
            queue: :queue.new()
end
