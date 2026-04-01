defmodule Goblin.MemTable.WAL do
  @moduledoc false

  alias Goblin.Log

  @log_key :wal

  defstruct [
    :log,
    :log_file
  ]

  @type t :: %__MODULE__{
          log: Log.t(),
          log_file: Path.t()
        }

  @spec open(atom(), Path.t(), boolean()) :: {:ok, t()} | {:error, term()}
  def open(name, path, write? \\ true) do
    mode =
      case write? do
        true -> :read_write
        false -> :read_only
      end

    with {:ok, log} <- Log.open({name, @log_key}, path, mode) do
      {:ok,
       %__MODULE__{
         log: log,
         log_file: path
       }}
    end
  end

  @spec append(t(), term()) :: {:ok, t()} | {:error, term()}
  def append(wal, writes) do
    with {:ok, _size} <- Log.append(wal.log, writes) do
      :ok
    end
  end

  @spec replay(t()) :: Enumerable.t()
  def replay(wal) do
    Log.stream_log!(wal.log)
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: Log.close(wal.log)

  @spec rm(t()) :: :ok | {:error, atom()}
  def rm(wal), do: File.rm(wal.log_file)

  @spec filepath(t()) :: Path.t()
  def filepath(wal), do: wal.log_file
end
