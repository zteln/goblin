defprotocol Goblin.Iterable do
  @moduledoc false
  @type t :: t()

  @doc "Initialize an iterator."
  @spec init(t()) :: t()
  def init(iter)

  @doc "Deinitialize an iterator, cleaning up any resources if necessary."
  @spec deinit(t()) :: :ok
  def deinit(iter)

  @doc "Perform an iteration, returning `:ok` if the iteration has ended."
  @spec next(t()) :: t() | :ok
  def next(iter)
end

defimpl Goblin.Iterable, for: List do
  def init(iterator), do: iterator
  def deinit(_iterator), do: :ok
  def next([]), do: :ok
  def next([hd | tl]), do: {hd, tl}
end
