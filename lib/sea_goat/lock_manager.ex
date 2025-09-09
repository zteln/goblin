defmodule SeaGoat.LockManager do
  # TODO: check if resource exists before locking: File.exists?
  use GenServer
  alias __MODULE__.Lock

  @public :public
  @private :private
  @lock_types [@public, @private]

  defstruct locks: %{},
            waiting: []

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def lock(server, resources, type, timeout \\ 5000)

  def lock(server, resources, type, timeout) when type in @lock_types do
    timeout = if type == @private, do: :infinity, else: timeout
    GenServer.call(server, {:lock, type, resources, self()}, timeout)
  end

  def lock(_, _, _, _), do: {:error, :invalid_lock_type}

  def unlock(server, resource) do
    GenServer.call(server, {:unlock, resource, self()})
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call({:lock, @public, resources, pid}, _from, state) do
    ref = monitor(pid)

    case add_locks(resources, pid, ref, @public, state.locks) do
      {:ok, locks} ->
        state = %{state | locks: locks}
        {:reply, :ok, state}

      {:error, _reason} = e ->
        demonitor(ref)
        {:reply, e, state}
    end
  end

  def handle_call({:lock, @private, resources, pid}, from, state) do
    ref = monitor(pid)

    case add_locks(resources, pid, ref, @private, state.locks) do
      {:ok, locks} ->
        state = %{state | locks: locks}
        {:reply, :ok, state}

      {:error, _reason} ->
        waiting = [{pid, ref, resources, from} | state.waiting]
        state = %{state | waiting: waiting}
        {:noreply, state}
    end
  end

  def handle_call({:unlock, resource, pid}, _from, state) do
    {ref, lock} =
      state.locks
      |> Map.get(resource)
      |> Lock.pop(pid)

    if ref, do: demonitor(ref)

    locks =
      if lock do
        Map.put(state.locks, resource, lock)
      else
        Map.delete(state.locks, resource)
      end

    state = %{state | locks: locks}
    {:reply, :ok, state, {:continue, :retry_waiting}}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    waiting = Enum.filter(state.waiting, &elem(&1, 0) != pid)
    locks = remove_pid_from_locks(state.locks, pid)
    state = %{state | locks: locks, waiting: waiting}
    {:noreply, state, {:continue, :retry_waiting}}
  end

  @impl GenServer
  def handle_continue(:retry_waiting, state) do
    {waiting, locks} = retry_waiting(Enum.reverse(state.waiting), state.locks)
    state = %{state | waiting: waiting, locks: locks}
    {:noreply, state}
  end

  defp remove_pid_from_locks(locks, pid) do
    locks
    |> Enum.flat_map(fn {resource, lock} ->
      {_ref, lock} = Lock.pop(lock, pid)
      if lock, do: [{resource, lock}], else: []
    end)
    |> Enum.into(%{})
  end

  defp retry_waiting(waiting, locks, acc \\ [])
  defp retry_waiting([], locks, acc), do: {acc, locks}

  defp retry_waiting([{pid, ref, resources, from} | waiting], locks, acc) do
    case add_locks(resources, pid, ref, @private, locks) do
      {:ok, locks} ->
        GenServer.reply(from, :ok)
        retry_waiting(waiting, locks, acc)

      {:error, _reason} ->
        retry_waiting(waiting, locks, [{pid, ref, resources, from} | acc])
    end
  end

  defp add_locks([], _pid, _ref, _type, acc), do: {:ok, acc}

  defp add_locks([resource | resources], pid, ref, type, acc) do
    case Map.get(acc, resource) do
      nil ->
        lock = Lock.make(type) |> Lock.add(pid, ref)
        acc = Map.put(acc, resource, lock)
        add_locks(resources, pid, ref, type, acc)

      %{type: @public} = lock when type == @public ->
        lock = Lock.add(lock, pid, ref)
        acc = Map.put(acc, resource, lock)
        add_locks(resources, pid, ref, type, acc)

      _ ->
        {:error, :locked}
    end
  end

  defp monitor(pid), do: Process.monitor(pid)
  defp demonitor(ref), do: Process.demonitor(ref)
end
