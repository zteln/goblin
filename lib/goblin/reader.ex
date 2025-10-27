defmodule Goblin.Reader do
  @moduledoc false
  alias Goblin.Writer
  alias Goblin.Store

  @task_timeout :timer.minutes(1)

  @spec get(Goblin.db_key(), Writer.writer(), Store.store(), non_neg_integer()) ::
          Goblin.db_value() | :not_found
  def get(key, writer, store, timeout \\ @task_timeout) do
    case try_writer(writer, key) do
      {:ok, {:value, seq, value}} ->
        {seq, value}

      :not_found ->
        try_store(store, key, timeout)
    end
  end

  @spec get_multi([Goblin.db_key()], Writer.writer(), Store.store(), non_neg_integer()) :: [
          {Goblin.db_key(), Goblin.db_sequence(), Goblin.db_value()} | :not_found
        ]
  def get_multi(keys, writer, store, timeout \\ @task_timeout) do
    {found, not_found} = try_writer(writer, keys)
    found ++ try_store(store, not_found, timeout)
  end

  # def select(writer, store, min, max) do
  #   Stream.resource(
  #     fn -> :ok end,
  #     fn _ -> :ok end,
  #     fn _ -> :ok end
  #   )
  # end

  defp try_writer(writer, keys) when is_list(keys), do: Writer.get_multi(writer, keys)
  defp try_writer(writer, key), do: Writer.get(writer, key)

  defp try_store(store, keys, timeout) when is_list(keys) do
    keys_and_ssts = Store.get(store, keys)

    keys_and_ssts
    |> Task.async_stream(fn {key, ssts} ->
      case async_read_ssts(ssts, timeout) do
        [] -> :not_found
        [{:value, seq, value}] -> {key, seq, value}
      end
    end)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Enum.to_list()
  end

  defp try_store(store, key, timeout) do
    [{_key, ssts}] = Store.get(store, key)

    case async_read_ssts(ssts, timeout) do
      [] -> :not_found
      [{:value, seq, value}] -> {seq, value}
    end
  end

  defp async_read_ssts(ssts, timeout) do
    result =
      ssts
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

    Enum.each(ssts, &elem(&1, 1).())
    result
  end
end
