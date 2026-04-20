defmodule Goblin.Mem do
  @moduledoc false

  alias Goblin.FileIO
  alias Goblin.Mem.Table

  defstruct [
    :io,
    :path,
    :table,
    :max_sequence
  ]

  @type t :: %__MODULE__{
          io: FileIO.t(),
          path: Path.t(),
          table: Table.t(),
          max_sequence: non_neg_integer()
        }

  @type commit :: {term(), non_neg_integer(), term()}

  @spec new(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(path, opts) do
    with {:ok, io} <- FileIO.open(path, write?: Keyword.get(opts, :write?, true)) do
      table = Table.new()

      max_seq =
        FileIO.stream!(io, truncate?: true)
        |> Enum.reduce(-1, fn {key, seq, val}, acc ->
          Table.insert(table, key, seq, val)
          max(acc, seq)
        end)

      {:ok, %__MODULE__{io: io, path: path, table: table, max_sequence: max_seq}}
    end
  end

  @spec append_commits(t(), list(commit())) :: {:ok, t()} | {:error, term()}
  def append_commits(mem, commits) do
    with {:ok, _size} <- FileIO.append(mem.io, commits),
         :ok <- FileIO.sync(mem.io) do
      max_seq =
        Enum.reduce(commits, mem.max_sequence, fn {key, seq, val}, _acc ->
          Table.insert(mem.table, key, seq, val)
          seq
        end)

      {:ok, %{mem | max_sequence: max_seq}}
    end
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(mem, key), do: Table.has_key?(mem.table, key)

  @spec search(t(), list(term()), non_neg_integer()) ::
          list({term(), non_neg_integer(), term()})
  def search(mem, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case Table.search(mem.table, key, seq) do
        {_, _, _} = triple -> [triple]
        _ -> []
      end
    end)
  end

  @spec stream(t(), non_neg_integer() | :infinity) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def stream(mem, max_seq) do
    Stream.resource(
      fn -> Table.iterate(mem.table) end,
      fn
        :end_of_iteration ->
          {:halt, nil}

        {key, seq} = idx when seq < max_seq ->
          case Table.get(mem.table, key, seq) do
            :not_found -> {[], Table.iterate(mem.table, idx)}
            triple -> {[triple], Table.iterate(mem.table, idx)}
          end

        idx ->
          {[], Table.iterate(mem.table, idx)}
      end,
      fn _ -> :ok end
    )
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(mem), do: FileIO.close(mem.io)

  @spec remove_disk(t()) :: :ok | {:error, term()}
  def remove_disk(mem), do: FileIO.remove(mem.path)

  @spec delete(t()) :: :ok
  def delete(mem), do: Table.delete(mem.table)

  @spec rotate?(t(), non_neg_integer()) :: boolean()
  def rotate?(mem, size_limit),
    do: Table.size(mem.table) >= size_limit

  @spec sequence(t()) :: non_neg_integer()
  def sequence(mem), do: mem.max_sequence + 1
end
