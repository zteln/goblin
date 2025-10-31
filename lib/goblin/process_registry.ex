defmodule Goblin.ProcessRegistry do
  @moduledoc false

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    Registry.start_link(keys: :unique, name: args[:name])
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(args) do
    Supervisor.child_spec(
      Registry,
      id: args[:name],
      start: {__MODULE__, :start_link, [args]}
    )
  end

  defmacro via(registry, key \\ nil) do
    quote do
      {:via, Registry, {unquote(registry), unquote(key) || __MODULE__}}
    end
  end
end
