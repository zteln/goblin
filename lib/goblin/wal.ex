defmodule Goblin.WAL do
  @moduledoc false
  alias Goblin.FileIO

  defstruct [:id, :io]

  @type t :: %__MODULE__{
          id: Path.t(),
          io: FileIO.t()
        }

  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
  def open(path) do
    with {:ok, io} <- FileIO.open(path, write?: true) do
      {:ok, %__MODULE__{id: path, io: io}}
    end
  end

  @spec delete(t()) :: :ok | {:error, term()}
  def delete(wal), do: FileIO.remove(wal.id)

  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: FileIO.close(wal.io)

  @spec append(t(), list(term())) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(wal, commits) do
    with {:ok, size} <- FileIO.append(wal.io, commits),
         :ok <- FileIO.sync(wal.io) do
      {:ok, size}
    end
  end

  @spec stream(t()) :: Enumerable.t()
  def stream(wal) do
    wal.io
    |> FileIO.stream()
    |> Stream.transform(nil, fn
      {:ok, commits}, acc ->
        {commits, acc}

      {:corrupt, pos}, acc ->
        FileIO.truncate(wal.io, pos)
        {:halt, acc}

      {:error, _reason}, acc ->
        {:halt, acc}
    end)
  end
end
