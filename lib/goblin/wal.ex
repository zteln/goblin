defmodule Goblin.WAL do
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

  @doc "Opens a WAL file at the given path."
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

  @doc "Appends writes to the WAL and syncs to disk."
  @spec append(t(), term()) :: {:ok, t()} | {:error, term()}
  def append(wal, writes) do
    with {:ok, _size} <- Log.append(wal.log, writes) do
      {:ok, wal}
    end
  end

  @doc "Returns a lazy stream of all entries in the WAL."
  @spec replay(t()) :: Enumerable.t()
  def replay(wal) do
    Log.stream_log!(wal.log)
  end

  @doc "Closes the WAL."
  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: Log.close(wal.log)

  @doc "Deletes the WAL file from disk."
  @spec rm(t()) :: :ok | {:error, atom()}
  def rm(wal), do: File.rm(wal.log_file)
end
