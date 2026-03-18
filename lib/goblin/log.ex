defmodule Goblin.Log do
  @moduledoc false

  @typedoc "A `:disk_log` handle."
  @type t :: any()

  @doc "Opens a disk log at the given file path with the specified mode."
  @spec open(term(), Path.t(), :read_write | :read_only) :: {:ok, t()} | {:error, term()}
  def open(name, file, mode \\ :read_write) do
    opts = [name: name, file: ~c"#{file}", quiet: true, mode: mode]

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      error -> error
    end
  end

  @doc "Closes a disk log."
  @spec close(t()) :: :ok | {:error, term()}
  def close(log), do: :disk_log.close(log)

  @doc """
  Appends term(s) to the log and syncs to disk.

  Returns `{:ok, bytes_written}` where `bytes_written` is the total
  external term format size of the appended terms.
  """
  @spec append(t(), term() | list(term())) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(log, terms) when is_list(terms) do
    with :ok <- :disk_log.log_terms(log, terms),
         :ok <- :disk_log.sync(log) do
      {:ok, Enum.reduce(terms, 0, &(&2 + :erlang.external_size(&1)))}
    end
  end

  def append(log, term), do: append(log, [term])

  @doc """
  Returns a lazy stream over all terms in the log.

  Raises on read errors.
  """
  @spec stream_log!(t()) :: Enumerable.t()
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
