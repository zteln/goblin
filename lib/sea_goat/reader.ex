defmodule SeaGoat.Reader do
  alias SeaGoat.Writer
  alias SeaGoat.Store

  @task_timeout :timer.minutes(1)

  @doc """
  Retrieves a value for the given key using a two-tier read strategy.

  First attempts to read from the writer (fast path), which typically contains
  recent writes and updates. If the key is not found in the writer, falls back
  to reading from the store's SSTables (slower path) for persistent data.

  ## Parameters

  - `writer` - GenServer process handling writes and serving recent data
  - `store` - GenServer process managing persistent storage (SS tables)  
  - `key` - The database key to retrieve

  ## Returns

  - The value associated with the key if found
  - `nil` if the key doesn't exist in either the writer or store

  ## Examples

      iex> SeaGoat.Reader.get(writer_pid, store_pid, "user:123")
      %{name: "John", age: 30}
      
      iex> SeaGoat.Reader.get(writer_pid, store_pid, "nonexistent")
      nil
  """
  @spec get(GenServer.server(), GenServer.server(), SeaGoat.db_key(), non_neg_integer()) ::
          SeaGoat.db_value() | :not_found
  def get(key, writer, store, timeout \\ @task_timeout) do
    case try_writer(writer, key) do
      {:ok, {:value, seq, value}} ->
        {seq, value}

      :not_found ->
        try_store(store, key, timeout)
    end
  end

  # def select(writer, store, min, max) do
  #
  # end

  defp try_writer(writer, key) do
    Writer.get(writer, key)
  end

  defp try_store(store, key, timeout) do
    ss_tables = Store.get_ss_tables(store, key)

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
