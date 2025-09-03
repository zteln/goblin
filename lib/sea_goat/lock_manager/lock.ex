defmodule SeaGoat.LockManager.Lock do
  defstruct [
    :type,
    keys: %{}
  ]

  @type t :: %__MODULE__{}

  @spec make(type :: :public | :private) :: t()
  def make(type) do
    %__MODULE__{type: type}
  end

  @spec add(lock :: t(), pid :: pid(), ref :: reference()) :: t()
  def add(lock, pid, ref) do
    %{lock | keys: Map.put(lock.keys, pid, ref)}
  end

  @spec pop(lock :: t(), pid :: pid()) :: {reference() | nil, t() | nil}
  def pop(lock, pid) do
    {ref, keys} = Map.pop(lock.keys, pid)

    if map_size(keys) == 0 do
      {ref, nil}
    else
      {ref, %{lock | keys: keys}}
    end
  end
end
