defmodule Goblin.MemTables.WAL do
  @moduledoc false

  @wal_suffix "wal"

  defstruct [
    :name,
    :file
  ]

  @type t :: %__MODULE__{}

  @doc "Open a new WAL. Can open a new from a previous WAL, incrementing the counter, or opens with default counter (0)."
  @spec open(t()) :: {:ok, t()} | {:error, term()}
  @spec open(atom(), Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(wal) do
    %{name: name, file: file} = wal
    open(name, next(file))
  end

  def open(name, file, opts \\ []) do
    mode =
      case Keyword.get(opts, :write?, true) do
        true -> :read_write
        false -> :read_only
      end

    opts = [name: name, file: ~c"#{file}", mode: mode]

    case :disk_log.open(opts) do
      {:ok, name} -> {:ok, %__MODULE__{name: name, file: file}}
      {:repaired, name, _recovered, _bad_bytes} -> {:ok, %__MODULE__{name: name, file: file}}
      error -> error
    end
  end

  @doc "Close the WAL."
  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: :disk_log.close(wal.name)

  @doc "Delete the WAL file on disk."
  @spec delete(t()) :: :ok | {:error, term()}
  def delete(wal), do: File.rm(wal.file)

  @doc "Append terms to the WAL, syncing the file synchronously afterwards."
  @spec append(t(), list(Goblin.write_term())) :: :ok | {:error, term()}
  def append(wal, writes) do
    with :ok <- :disk_log.log_terms(wal.name, writes),
         :ok <- :disk_log.sync(wal.name) do
      :ok
    end
  end

  @doc "Stream the contents of the WAL."
  @spec stream_log!(t()) :: Enumerable.t(Goblin.write_term())
  def stream_log!(wal) do
    Stream.resource(
      fn -> :disk_log.chunk(wal.name, :start) end,
      fn
        :eof -> {:halt, :eof}
        {:error, _reason} = error -> raise "Failed to stream log, reason: #{inspect(error)}"
        {continuation, terms} -> {terms, :disk_log.chunk(wal.name, continuation)}
      end,
      fn _ -> :ok end
    )
  end

  @doc "Get the full path for a WAL."
  @spec filename(Path.t(), non_neg_integer()) :: Path.t()
  def filename(dir, n \\ 0) do
    hex_n = Integer.to_string(n, 16)
    Path.join(dir, "#{String.pad_leading(hex_n, 20, "0")}.#{@wal_suffix}")
  end

  defp next(path) do
    dir = Path.dirname(path)

    [n, _suffix] =
      path
      |> Path.basename()
      |> String.split(".")

    next_n = String.to_integer(n) + 1
    filename(dir, next_n)
  end
end
