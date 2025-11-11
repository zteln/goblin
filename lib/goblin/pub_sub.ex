defmodule Goblin.PubSub do
  @moduledoc false

  @topic :db_writes

  @typep pub_sub :: module()

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

  @spec subscribe(pub_sub()) :: {:ok, pid()} | {:error, term()}
  def subscribe(pub_sub) do
    Registry.register(pub_sub, @topic, [])
  end

  @spec unsubscribe(pub_sub()) :: :ok
  def unsubscribe(pub_sub) do
    Registry.unregister(pub_sub, @topic)
  end

  @spec publish(pub_sub(), term()) :: :ok
  def publish(pub_sub, writes) do
    Registry.dispatch(pub_sub, @topic, fn entries ->
      for {pid, _} <- entries do
        for write <- writes, do: send(pid, write)
      end
    end)
  end
end
