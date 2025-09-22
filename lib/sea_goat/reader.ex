defmodule SeaGoat.Reader do
  alias SeaGoat.Writer
  alias SeaGoat.Store

  @task_timeout :timer.minutes(1)

  def get(writer, store, key) do
    case try_writer(writer, key) do
      {:ok, value} ->
        value

      :error ->
        try_store(store, key)
    end
  end

  defp try_writer(writer, key) do
    Writer.read(writer, key)
  end

  defp try_store(store, key) do
    ss_tables = Store.get_ss_tables(store, key)

    result =
      ss_tables
      |> Stream.map(&elem(&1, 0))
      |> Task.async_stream(& &1.(), timeout: @task_timeout)
      |> Stream.map(fn {:ok, res} -> res end)
      |> Stream.filter(& &1)
      |> Stream.take(1)
      |> Enum.to_list()

    ss_tables
    |> Enum.each(&elem(&1, 1).())

    case result do
      [ok: {:value, value}] ->
        value

      _ ->
        nil
    end
  end
end
