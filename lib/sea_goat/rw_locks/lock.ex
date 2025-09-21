defmodule SeaGoat.RWLocks.Lock do
  defstruct [
    :waiting,
    current: []
  ]

  # returns 
  # {:locked, lock} -> acquired lock
  # {:busy, lock} -> reader/writer did not acquire lock
  # {:waiting, lock} -> writer waiting for lock
  def lock(lock, :r, locker) do
    case lock.current do
      [{:wlock, _}] ->
        {:busy, lock}

      current ->
        current = [{:rlock, locker} | current]
        lock = %{lock | current: current}
        {:locked, lock}
    end
  end

  def lock(lock, :w, locker) do
    case lock do
      %{current: [], waiting: nil} ->
        current = [{:wlock, locker}]
        lock = %{lock | current: current}
        {:locked, lock}

      %{waiting: nil} ->
        waiting = {:wlock, locker}
        lock = %{lock | waiting: waiting}
        {:waiting, lock}

      _ ->
        {:busy, lock}
    end
  end

  # returns
  # {:unlocked, lock} -> lock released
  # {:released, locker, lock} -> writer acquired lock
  def unlock(lock, on_match) do
    current =
      Enum.reject(lock.current, fn
        {_type, locker} -> on_match.(locker)
      end)

    waiting =
      case lock.waiting do
        {:wlock, locker} = waiting ->
          if on_match.(locker), do: nil, else: waiting

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
    {:wlock, locker} = lock.waiting
    current = [lock.waiting]
    waiting = nil
    lock = %{lock | current: current, waiting: waiting}
    {:released, locker, lock}
  end
end
