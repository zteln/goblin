defmodule SeaGoat.LockManager do
  use GenServer

  defstruct shared_locks: %{},
            exclusive_locks: %{},
            in_waiting: %{}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def acquire_shared_lock(server, resource) do
    GenServer.call(server, {:shared, resource, self()})
  end

  def acquire_exclusive_lock(server, resource) do
    GenServer.call(server, {:exclusive, resource, self()}, :infinity)
  end

  def release_lock(server, resource) do
    GenServer.call(server, {:release, resource, self()})
  end

  def lock_status(server, resource) do
    GenServer.call(server, {:status, resource})
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call({:shared, resource, client}, _from, state) do
    cond do
      Map.has_key?(state.exclusive_locks, resource) ->
        {:reply, {:error, :not_shareable}, state}

      true ->
        ref = monitor_client(client)

        shared_locks =
          Map.update(state.shared_locks, resource, %{client => ref}, &Map.put(&1, client, ref))

        state = %{state | shared_locks: shared_locks}
        {:reply, :ok, state}
    end
  end

  def handle_call({:exclusive, resource, client}, from, state) do
    if Map.has_key?(state.shared_locks, resource) or Map.has_key?(state.exclusive_locks, resource) do
      in_waiting =
        Map.update(
          state.in_waiting,
          resource,
          :queue.from_list([{client, from}]),
          &:queue.in({client, from}, &1)
        )

      {:noreply, %{state | in_waiting: in_waiting}}
    else
      ref = monitor_client(client)
      exclusive_locks = Map.put(state.exclusive_locks, resource, {client, ref})
      state = %{state | exclusive_locks: exclusive_locks}
      {:reply, :ok, state}
    end
  end

  def handle_call({:release, resource, client}, _from, state) do
    state =
      cond do
        Map.has_key?(state.shared_locks, resource) ->
          resource_locks =
            state.shared_locks
            |> Map.get(resource, %{})
            |> Enum.filter(fn
              {^client, ref} ->
                demonitor_client(ref)
                false

              _ ->
                true
            end)
            |> Enum.into(%{})

          if map_size(resource_locks) == 0 do
            shared_locks = Map.delete(state.shared_locks, resource)

            %{state | shared_locks: shared_locks}
            |> release_to_waiting_client(resource)
          else
            shared_locks = Map.put(state.shared_locks, resource, resource_locks)
            %{state | shared_locks: shared_locks}
          end

        Map.has_key?(state.exclusive_locks, resource) ->
          {^client, ref} = Map.get(state.exclusive_locks, resource)
          demonitor_client(ref)
          exclusive_locks = Map.delete(state.exclusive_locks, resource)

          %{state | exclusive_locks: exclusive_locks}
          |> release_to_waiting_client(resource)

        true ->
          state
      end

    {:reply, :ok, state}
  end

  def handle_call({:status, resource}, _from, state) do
    reply =
      cond do
        Map.has_key?(state.shared_locks, resource) -> :in_use
        Map.has_key?(state.exclusive_locks, resource) -> :in_use
        true -> :free
      end

    {:reply, reply, state}
  end

  defp release_to_waiting_client(state, resource) do
    if Map.has_key?(state.in_waiting, resource) do
      queue = Map.get(state.in_waiting, resource)

      case :queue.out(queue) do
        {:empty, _queue} ->
          in_waiting = Map.delete(state.in_waiting, resource)
          %{state | in_waiting: in_waiting}

        {{:value, {client, from}}, queue} ->
          GenServer.reply(from, :ok)
          ref = monitor_client(client)
          exclusive_locks = Map.put(state.exclusive_locks, resource, {client, ref})
          in_waiting = Map.put(state.in_waiting, resource, queue)
          %{state | in_waiting: in_waiting, exclusive_locks: exclusive_locks}
      end
    else
      state
    end
  end

  defp monitor_client(client), do: Process.monitor(client)
  defp demonitor_client(client), do: Process.demonitor(client)
end
