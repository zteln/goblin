defmodule Talon.Reader do
  @moduledoc false
  alias Talon.Writer
  alias Talon.Store

  @task_timeout :timer.minutes(1)

  @spec get(GenServer.server(), GenServer.server(), Talon.db_key(), non_neg_integer()) ::
          Talon.db_value() | :not_found
  def get(key, writer, store, timeout \\ @task_timeout) do
    case try_writer(writer, key) do
      {:ok, {:value, seq, value}} ->
        {seq, value}

      :not_found ->
        try_store(store, key, timeout)
    end
  end

  # def select(writer, store, min, max) do
  #   Stream.resource(
  #     fn -> :ok end,
  #     fn _ -> :ok end,
  #     fn _ -> :ok end
  #   )
  # end

  defp try_writer(writer, key) do
    Writer.get(writer, key)
  end

  defp try_store(store, key, timeout) do
    ss_tables = Store.get(store, key)

    result =
      ss_tables
      |> Stream.map(&elem(&1, 0))
      |> Task.async_stream(& &1.(), timeout: timeout)
      |> Stream.map(fn {:ok, res} -> res end)
      |> Stream.map(fn
        {:error, reason} ->
          raise "Failed to read, reason: #{inspect(reason)}"

        res ->
          res
      end)
      |> Stream.filter(&match?({:ok, _}, &1))
      |> Stream.map(fn {:ok, res} -> res end)
      |> Enum.sort_by(&elem(&1, 1), :desc)
      |> Enum.take(1)

    ss_tables
    |> Enum.each(&elem(&1, 1).())

    case result do
      [] -> :not_found
      [{:value, seq, value}] -> {seq, value}
    end
  end
end
