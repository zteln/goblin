defmodule Goblin.PubSub do
  @moduledoc false

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    Registry.start_link(
      name: args[:name],
      keys: :duplicate,
      partitions: System.schedulers_online()
    )
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(args) do
    Supervisor.child_spec(
      Registry,
      id: args[:name],
      start: {__MODULE__, :start_link, [args]}
    )
  end

  @spec subscribe(Goblin.server()) :: {:ok, pid()} | {:error, term()}
  def subscribe(pub_sub) do
    Registry.register(pub_sub, pub_sub, [])
  end

  @spec unsubscribe(Goblin.server()) :: :ok
  def unsubscribe(pub_sub) do
    Registry.unregister(pub_sub, pub_sub)
  end

  @spec publish(Goblin.server(), [
          {:put, Goblin.db_key(), Goblin.db_value()} | {:remove, Goblin.db_key()}
        ]) :: :ok
  def publish(pub_sub, writes) do
    Registry.dispatch(pub_sub, pub_sub, fn entries ->
      for {pid, _} <- entries do
        for write <- writes, do: send(pid, write)
      end
    end)
  end
end
