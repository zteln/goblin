defmodule SeaGoat.RWLocks.Lock do
  @moduledoc """
  Lock implementation for a simple reader-writer lock.
  Reader locks are always acquired as long as no writer lock has acquired the lock.
  Writer locks wait until all reader locks are released.
  """
  defstruct [
    :waiting,
    current: []
  ]

  @type t :: %__MODULE__{}
  @type id :: term()
  @type lock_type :: :r | :w

  @doc """
  Update the lock with either a read-lock or write-lock with corresponding `id`.
  If the lock is currently held by a write-lock, then new locks are not set to waiting and `{:busy, lock}` is returned with `lock` unchanged.
  If the lock is currently held by read-locks, then new read-locks are added to the lock (returning `{:locked, lock}`) while any write-lock is set to waiting (returning `{:waiting, lock}`) unless waiting is already set (returning `{:busy, lock}`).
  """
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

  @doc """
  Unlocks a lock by removing any currently held locks or waiting lock.
  If the waiting lock acquires the lock then `{:released, id, lock}` is returned, with `id` the ID of the previously waiting lock.
  If a lock is removed from the current list of locks without releasing to the waiting lock then `{:unlocked, lock}` is returned.
  If a lock becomes empty, i.e. not held locks nor waiting lock, then `:empty` is returned.
  """
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
