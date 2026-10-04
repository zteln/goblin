defmodule Goblin.WAL do
  @moduledoc false
  alias Goblin.Persistence

  defstruct [:id, :io]

  @type t :: %__MODULE__{
          id: Path.t(),
          io: Persistence.t()
        }

  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
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

  @spec stream(t()) :: Enumerable.t({:ok, term()} | {:error, term()})
  def stream(wal) do
    wal.io
    |> Persistence.stream()
    |> Stream.transform(nil, fn
      {:ok, commits}, acc ->
        {[{:ok, commits}], acc}

      {:corrupt, pos}, acc ->
        Persistence.truncate(wal.io, pos)
        {:halt, acc}

      {:error, _reason} = error, _acc ->
        {[error], :halt}
    end)
  end
end
