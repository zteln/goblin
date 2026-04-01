defmodule Goblin.MemTable do
  @moduledoc false

  alias Goblin.MemTable.{
    WAL,
    Store
  }

  defstruct [
    :wal,
    :store
  ]

  @type t :: %__MODULE__{
          wal: WAL.t(),
          store: Store.t()
        }

  @type commit ::
          {:put, non_neg_integer(), term(), term()}
          | {:remove, non_neg_integer(), term()}

  @spec open(any(), Path.t(), keyword()) :: {:ok, t()}
  def open(name, path, opts) do
    sequence = opts[:sequence]

    with {:ok, wal} <- WAL.open(name, path, opts[:write?] || true) do
      store = Store.new()

      WAL.replay(wal)
      |> Stream.filter(fn
        {:put, seq, _key, _value} -> seq <= sequence
        {:remove, seq, _key} -> seq <= sequence
      end)
      |> Enum.each(&update_store(store, &1))

      {:ok, %__MODULE__{wal: wal, store: store}}
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(mem_table) do
    WAL.close(mem_table.wal)
  end

  @spec remove_wal(t()) :: :ok | {:error, term()}
  def remove_wal(mem_table), do: WAL.rm(mem_table.wal)

  @spec delete_table(t()) :: :ok
  def delete_table(mem_table), do: Store.delete(mem_table.store)

  @spec wal_path(t()) :: Path.t()
  def wal_path(mem_table) do
    WAL.filepath(mem_table.wal)
  end

  @spec rotate?(t(), non_neg_integer()) :: boolean()
  def rotate?(mem_table, size_limit) do
    Store.size(mem_table.store) >= size_limit
  end

  @spec append_commits(t(), list(commit())) :: :ok | {:error, term()}
  def append_commits(mem_table, commits) do
    with :ok <- WAL.append(mem_table.wal, commits) do
      Enum.each(commits, &update_store(mem_table.store, &1))
    end
  end

  defp update_store(store, {:put, seq, key, value}),
    do: Store.insert(store, key, seq, value)

  defp update_store(store, {:remove, seq, key}),
    do: Store.remove(store, key, seq)
end

defimpl Goblin.Queryable, for: Goblin.MemTable do
  alias Goblin.MemTable.{Store, Iterator}

  def has_key?(mem_table, key) do
    Store.has_key?(mem_table.store, key)
  end

  def search(mem_table, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case Store.search(mem_table.store, key, seq) do
        {_key, _seq, _value} = triple -> [triple]
        _ -> []
      end
    end)
  end

  def stream(mem_table, _min, _max, seq) do
    %Iterator{store: mem_table.store, max_seq: seq}
  end
end
