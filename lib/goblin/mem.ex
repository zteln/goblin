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

  @type commit ::
          {:put, non_neg_integer(), term(), term()}
          | {:remove, non_neg_integer(), term()}

  @spec new(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(path, opts) do
    with {:ok, io} <- FileIO.open(path, write?: Keyword.get(opts, :write?, true)) do
      table = Table.new()

      max_seq =
        FileIO.stream!(io, truncate?: true)
        |> Enum.reduce(-1, fn entry, acc ->
          update_table(table, entry)
          max(acc, elem(entry, 1))
        end)

      {:ok, %__MODULE__{io: io, path: path, table: table, max_sequence: max_seq}}
    end
  end

  def append_commits(mem, commits) do
    with {:ok, _size} <- FileIO.append(mem.io, commits),
         :ok <- FileIO.sync(mem.io) do
      max_seq =
        Enum.reduce(commits, mem.max_sequence, fn commit, _acc ->
          update_table(mem.table, commit)
          elem(commit, 1)
        end)

      {:ok, %{mem | max_sequence: max_seq}}
    end
  end

  def disk_path(mem), do: mem.path
  def close(mem), do: FileIO.close(mem.io)
  def remove_disk(mem), do: FileIO.remove(mem.path)

  def rotate?(mem, size_limit),
    do: Table.size(mem.table) >= size_limit

  def sequence(mem), do: mem.max_sequence + 1

  defp update_table(store, {:put, seq, key, value}),
    do: Table.insert(store, key, seq, value)

  defp update_table(store, {:remove, seq, key}),
    do: Table.remove(store, key, seq)

  defimpl Goblin.Brokerable do
    alias Goblin.Mem

    def id(mem), do: Mem.disk_path(mem)
    def level_key(_mem), do: -1
    def remove(mem), do: Mem.Table.delete(mem.table)
  end

  defimpl Goblin.Queryable do
    alias Goblin.Mem.{Table, Iterator}

    def has_key?(mem, key), do: Table.has_key?(mem.table, key)

    def search(mem, keys, seq) do
      Enum.flat_map(keys, fn key ->
        case Table.search(mem.table, key, seq) do
          {_, _, _} = triple -> [triple]
          _ -> []
        end
      end)
    end

    def stream(mem, _min, _max, seq), do: Iterator.new(mem, seq)
  end
end
