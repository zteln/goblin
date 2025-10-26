defmodule Goblin.RWLocks.Lock do
  @moduledoc false
  defstruct [
    :waiting,
    current: []
  ]

  @type t :: %__MODULE__{}
  @type id :: term()
  @type lock_type :: :r | :w

  @spec lock(t(), lock_type(), id()) :: {:locked | :busy | :waiting, t()}
  def lock(%__MODULE__{} = lock, :r, id) do
    case lock.current do
      [{:wlock, _}] ->
        {:busy, lock}

      current ->
        current = [{:rlock, id} | current]
        lock = %{lock | current: current}
        {:locked, lock}
    end
  end

  def lock(%__MODULE__{} = lock, :w, id) do
    case lock do
      %{current: [], waiting: nil} ->
        current = [{:wlock, id}]
        lock = %{lock | current: current}
        {:locked, lock}

      %{current: [{:wlock, _id}]} ->
        {:busy, lock}

      %{waiting: nil} ->
        waiting = {:wlock, id}
        lock = %{lock | waiting: waiting}
        {:waiting, lock}

      _ ->
        {:busy, lock}
    end
  end

  @spec unlock(t(), (id() -> boolean())) ::
          :empty | {:unlocked, t()} | {:released, id(), t()}
  def unlock(lock, on_match) do
    current =
      Enum.reject(lock.current, fn
        {_type, id} -> on_match.(id)
      end)

    waiting =
      case lock.waiting do
        {:wlock, id} = waiting ->
          if on_match.(id), do: nil, else: waiting

        waiting ->
          waiting
      end

    %{lock | current: current, waiting: waiting}
    |> release_to_waiting()
  end

  defp release_to_waiting(%{current: [], waiting: nil}), do: :empty

  defp release_to_waiting(%{current: current} = lock) when length(current) > 0,
    do: {:unlocked, lock}

  defp release_to_waiting(%{waiting: nil} = lock), do: {:unlocked, lock}

  defp release_to_waiting(lock) do
    {:wlock, id} = lock.waiting
    current = [lock.waiting]
    waiting = nil
    lock = %{lock | current: current, waiting: waiting}
    {:released, id, lock}
  end
end
