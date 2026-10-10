defmodule Goblin.WAL do
  @moduledoc false
  alias Goblin.Persistence

  defstruct [:id, :io]

  @type t :: %__MODULE__{
          id: Path.t(),
          io: Persistence.t()
        }

  @spec open(Path.t(), boolean()) :: {:ok, t()} | {:error, term()}
  def open(path, new? \\ false) do
    with {:ok, io} <- Persistence.open(path, write?: true, new?: new?) do
      {:ok, %__MODULE__{id: path, io: io}}
    end
  end

  @spec delete(t()) :: :ok | {:error, term()}
  def delete(wal), do: Persistence.remove(wal.id)

  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: Persistence.close(wal.io)

  @spec append(t(), list(term())) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(wal, commits) do
    with {:ok, size} <- Persistence.append(wal.io, commits),
         :ok <- Persistence.sync(wal.io) do
      {:ok, size}
    end
  end

  @spec replay(t(), term(), (term(), term() -> term())) :: {:ok, term()} | {:error, term()}
  def replay(wal, acc, fun) do
    with :ok <- Persistence.set_position(wal.io, 0),
         do: do_replay(wal, acc, fun)
  end

  defp do_replay(wal, acc, fun) do
    case Persistence.seq_read(wal.io) do
      {:ok, commits} -> do_replay(wal, fun.(commits, acc), fun)
      {:error, :eof} -> {:ok, acc}
      {:error, {:corrupt, pos}} -> with :ok <- Persistence.truncate(wal.io, pos), do: {:ok, acc}
      error -> error
    end
  end
end
