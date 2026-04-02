defmodule Goblin.MemTable.WAL do
  @moduledoc false

  @log_key :wal

  defstruct [
    :log,
    :log_file
  ]

  @type t :: %__MODULE__{
          log: term(),
          log_file: Path.t()
        }

  @spec open(atom(), Path.t(), boolean()) :: {:ok, t()} | {:error, term()}
  def open(name, log_file, write? \\ true) do
    mode =
      case write? do
        true -> :read_write
        false -> :read_only
      end

    opts = [
      name: {name, @log_key},
      file: ~c"#{log_file}",
      quiet: true,
      mode: mode
    ]

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, %__MODULE__{log: log, log_file: log_file}}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, %__MODULE__{log: log, log_file: log_file}}
      error -> error
    end
  end

  @spec append(t(), term()) :: {:ok, t()} | {:error, term()}
  def append(wal, writes) do
    with :ok <- :disk_log.log_terms(wal.log, writes) do
      :disk_log.sync(wal.log)
    end
  end

  @spec replay(t()) :: Enumerable.t()
  def replay(wal) do
    Stream.resource(
      fn -> :disk_log.chunk(wal.log, :start) end,
      fn
        :eof -> {:halt, :eof}
        {:error, reason} -> raise "Failed to stream log, reason: #{inspect(reason)}"
        {continuation, terms} -> {terms, :disk_log.chunk(wal.log, continuation)}
      end,
      fn _ -> :ok end
    )
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: :disk_log.close(wal.log)

  @spec rm(t()) :: :ok | {:error, atom()}
  def rm(wal), do: File.rm(wal.log_file)

  @spec filepath(t()) :: Path.t()
  def filepath(wal), do: wal.log_file
end
