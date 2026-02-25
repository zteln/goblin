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

  @doc "Subscribe to the database registry, receiving write updates."
  @spec subscribe(Registry.registry()) :: {:ok, pid()} | {:error, term()}
  def subscribe(pub_sub) do
    Registry.register(pub_sub, pub_sub, [])
  end

  @doc "Unsubscribe from the database registry."
  @spec unsubscribe(Registry.registry()) :: :ok
  def unsubscribe(pub_sub) do
    Registry.unregister(pub_sub, pub_sub)
  end

  @doc "Publish to the database registry."
  @spec publish(Registry.registry(), list(Goblin.write_term())) :: :ok
  def publish(pub_sub, writes) do
    Registry.dispatch(pub_sub, pub_sub, fn entries ->
      for {pid, _} <- entries do
        for write <- writes, do: send(pid, write)
      end
    end)
  end
end
