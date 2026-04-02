defmodule Goblin.Manifest.Log do
  @moduledoc false
  require Logger

  @log_size {10 * 1024 * 1024, 2}

  @spec open_log(term(), Path.t()) :: {:ok, term()} | {:error, term()}
  def open_log(name, file) do
    opts = [
      name: name,
      file: ~c"#{file}",
      quiet: true,
      mode: :read_write,
      type: :wrap,
      size: @log_size
    ]

    case :disk_log.open(opts) do
      {:ok, log} ->
        {:ok, log}

      {:repaired, log, recovered, bad_bytes} ->
        Logger.warning(fn ->
          "[#{inspect(__MODULE__)}] repaired manifest log, recovered: #{inspect(recovered)}, bad bytes: #{inspect(bad_bytes)}."
        end)

        {:ok, log}

      error ->
        error
    end
  end

  @spec close_log(term()) :: :ok | {:error, term()}
  def close_log(log), do: :disk_log.close(log)

  @spec set_header(term(), term()) :: :ok | {:error, term()}
  def set_header(log, header) do
    with :ok <- :disk_log.change_header(log, {:head, header}) do
      :disk_log.sync(log)
    end
  end

  @spec append(term(), list(term()), term()) :: :ok | {:error, term()}
  def append(log, terms, header) do
    with :ok <- :disk_log.change_header(log, {:head, header}),
         :ok <- :disk_log.log_terms(log, terms) do
      :disk_log.sync(log)
    end
  end

  @spec stream_log!(term()) :: Enumerable.t(term())
  def stream_log!(log) do
    Stream.resource(
      fn -> :disk_log.chunk(log, :start) end,
      fn
        :eof -> {:halt, :eof}
        {:error, reason} -> raise "Failed to stream log, reason: #{inspect(reason)}"
        {continuation, terms} -> {terms, :disk_log.chunk(log, continuation)}
      end,
      fn _ -> :ok end
    )
  end
end
