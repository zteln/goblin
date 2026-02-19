defprotocol Goblin.Iterable do
  @moduledoc false
  @type t :: t()

  @spec init(t()) :: t()
  def init(iter)

  @spec deinit(t()) :: :ok
  def deinit(iter)

  @spec next(t()) :: t() | :ok
  def next(iter)
end

defimpl Goblin.Iterable, for: List do
  def init(iterator), do: iterator
  def deinit(_iterator), do: :ok

  def next([]), do: :ok
  def next([hd | tl]), do: {hd, tl}
end
