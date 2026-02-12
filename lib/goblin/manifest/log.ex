defmodule Goblin.Manifest.Log do
  @moduledoc false

  @doc "Opens a new log."
  @spec open(atom(), Path.t()) :: {:ok, atom()} | {:error, term()}
  def open(name, file) do
    opts = [name: name, file: ~c"#{file}"]

    case :disk_log.open(opts) do
      {:ok, _log} -> {:ok, name}
      {:repaired, _log, _recovered, _bad_bytes} -> {:ok, name}
      error -> error
    end
  end

  @doc "Closes a log."
  @spec close(atom()) :: :ok | {:error, term()}
  def close(log), do: :disk_log.close(log)

  @doc "Appends term(s) to the log."
  @spec append(atom(), term() | list(term())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def append(log, terms) when is_list(terms) do
    with :ok <- :disk_log.log_terms(log, terms),
         :ok <- :disk_log.sync(log) do
      {:ok, Enum.reduce(terms, 0, &(&2 + :erlang.external_size(&1)))}
    end
  end

  def append(log, term), do: append(log, [term])

  @doc "Streams the entire content of the log."
  @spec stream_log!(atom()) :: Enumerable.t()
  def stream_log!(log) do
    Stream.resource(
      fn -> :disk_log.chunk(log, :start) end,
      fn
        :eof ->
          {:halt, :eof}

        {:error, _reason} = error ->
          raise "Failed to stream log, reason: #{inspect(error)}"

        {continuation, terms} ->
          {terms, :disk_log.chunk(log, continuation)}
      end,
      fn _ -> :ok end
    )
  end
end
