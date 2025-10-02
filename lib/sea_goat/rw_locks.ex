defmodule SeaGoat.RWLocks do
  @moduledoc """
  Management of reader-writer locks.
  Locks are PID-based and all processes acquiring a lock are monitored.
  """
  use GenServer
  alias __MODULE__.Lock

  @type resource :: term()

  defstruct locks: %{}

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, nil, name: opts[:name])
  end

  @doc """
  Attempts to acquire a write-lock on `resource`.
  If there is no other writer waiting, then the caller is blocked until the lock is acquired and returns `:ok`.
  If there is another writer waiting already, then `{:error, :lock_in_use}` is returned immediately.
  """
  @spec wlock(GenServer.server(), resource()) :: :ok | {:error, :lock_in_use}
  def wlock(rw_locks, resource) do
    GenServer.call(rw_locks, {:wlock, resource}, :infinity)
  end

  @doc """
  Attempts to acquire a read-lock on `resource`.
  If no writer currently holds the lock, then `rlock` succeeds and returns `:ok`.
  If the lock is held by a writer, then `{:error, :lock_in_use}` is returned.
  """
  @spec rlock(GenServer.server(), resource()) :: :ok | {:error, :lock_in_use}
  def rlock(rw_locks, resource, pid \\ self()) do
    GenServer.call(rw_locks, {:rlock, resource, pid})
  end

  @doc """
  Unlocks the lock held by the process. Returns `:ok`.
  If the process is the last reader to hold the lock, then the lock is released to a waiting writer, if one exists.
  """
  @spec unlock(GenServer.server(), resource()) :: :ok
  def unlock(rw_locks, resource, pid \\ self()) do
    GenServer.call(rw_locks, {:unlock, resource, pid})
  end

  @impl GenServer
  def init(_args) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call({:wlock, resource}, {pid, ref}, state) do
    lock = Map.get(state.locks, resource, %Lock{})
    monitor_ref = Process.monitor(pid)

    case Lock.lock(lock, :w, {pid, ref, monitor_ref}) do
      {:busy, lock} ->
        Process.demonitor(monitor_ref)
        locks = Map.put(state.locks, resource, lock)
        state = %{state | locks: locks}
        {:reply, {:error, :lock_in_use}, state}

      {:waiting, lock} ->
        locks = Map.put(state.locks, resource, lock)
        state = %{state | locks: locks}
        {:noreply, state}

      {:locked, lock} ->
        locks = Map.put(state.locks, resource, lock)
        state = %{state | locks: locks}
        {:reply, :ok, state}
    end
  end

  def handle_call({:rlock, resource, pid}, _from, state) do
    lock = Map.get(state.locks, resource, %Lock{})
    monitor_ref = Process.monitor(pid)

    case Lock.lock(lock, :r, {pid, nil, monitor_ref}) do
      {:busy, lock} ->
        Process.demonitor(monitor_ref)
        locks = Map.put(state.locks, resource, lock)
        state = %{state | locks: locks}
        {:reply, {:error, :lock_in_use}, state}

      {:locked, lock} ->
        locks = Map.put(state.locks, resource, lock)
        state = %{state | locks: locks}
        {:reply, :ok, state}
    end
  end

  def handle_call({:unlock, resource, pid}, _from, state) do
    lock = Map.get(state.locks, resource, %Lock{})

    state =
      case do_unlock(lock, &unlock_by_pid(&1, pid)) do
        nil ->
          locks = Map.delete(state.locks, resource)
          %{state | locks: locks}

        lock ->
          locks = Map.put(state.locks, resource, lock)
          %{state | locks: locks}
      end

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, :process, pid, _reason}, state) do
    locks =
      state.locks
      |> Enum.flat_map(fn {resource, lock} ->
        case do_unlock(lock, &unlock_by_pid_and_monitor_ref(&1, pid, monitor_ref)) do
          nil ->
            []

          lock ->
            [{resource, lock}]
        end
      end)
      |> Enum.into(%{})

    %{state | locks: locks}
    {:noreply, state}
  end

  defp do_unlock(lock, on_match) do
    case Lock.unlock(lock, on_match) do
      :empty ->
        nil

      {:unlocked, lock} ->
        lock

      {:released, {pid, ref, _monitor_ref}, lock} ->
        GenServer.reply({pid, ref}, :ok)
        lock
    end
  end

  defp unlock_by_pid({pid, _ref, monitor_ref}, matching_pid) do
    if pid == matching_pid do
      Process.demonitor(monitor_ref)
    end
  end

  defp unlock_by_pid_and_monitor_ref(
         {pid, _ref, monitor_ref},
         matching_pid,
         matching_monitor_ref
       ) do
    if pid == matching_pid and monitor_ref == matching_monitor_ref do
      Process.demonitor(monitor_ref)
    end
  end
end
