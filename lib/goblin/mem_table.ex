defmodule Goblin.MemTable do
  @moduledoc false

  alias Goblin.FileIO

  defstruct [
    :id,
    :io,
    :ref,
    :max_sequence,
    :mem_limit,
    :filer
  ]

  @type t :: %__MODULE__{
          id: Path.t(),
          io: FileIO.t(),
          ref: :ets.table(),
          max_sequence: non_neg_integer(),
          mem_limit: non_neg_integer(),
          filer: (-> Path.t())
        }

  @spec new(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(path, opts) do
    with {:ok, io} <- FileIO.open(path, write?: true) do
      ref = new_table()

      max_seq =
        FileIO.stream!(io, truncate?: true)
        |> insert_commits(ref)

      {:ok,
       %__MODULE__{
         io: io,
         id: path,
         ref: ref,
         max_sequence: max_seq + 1,
         mem_limit: opts[:mem_limit],
         filer: opts[:filer]
       }}
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(mt), do: FileIO.close(mt.io)

  @spec destroy(t()) :: :ok | {:error, term()}
  def destroy(mt) do
    with :ok <- FileIO.remove(mt.io) do
      :ets.delete(mt.ref)
      :ok
    end
  end

  @spec append(t(), list({term(), non_neg_integer(), term()})) :: {:ok, t()} | {:error, term()}
  def append(mem_table, commits) do
    with {:ok, _size} <- FileIO.append(mem_table.io, commits),
         :ok <- FileIO.sync(mem_table.io) do
      max_seq = insert_commits(commits, mem_table.ref)
      mem_table = %{mem_table | max_sequence: max_seq + 1}
      maybe_rotate(mem_table)
    end
  end

  @spec has_key?(t(), term()) :: boolean()
  def has_key?(mem_table, key) do
    table_has_key?(mem_table.ref, key)
  end

  @spec search(t(), list(term()), non_neg_integer()) :: list({term(), non_neg_integer(), term()})
  def search(mem_table, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case search_table(mem_table.ref, key, seq) do
        nil -> []
        triple -> [triple]
      end
    end)
  end

  @spec stream(t(), non_neg_integer() | :infinity) ::
          Enumerable.t({term(), non_neg_integer(), term()})
  def stream(mem_table, max_seq \\ :infinity) do
    Stream.resource(
      fn -> iterate(mem_table.ref) end,
      fn
        :end_of_iteration ->
          {:halt, nil}

        {key, seq} = idx when seq < max_seq ->
          case get(mem_table.ref, key, seq) do
            nil -> {[], iterate(mem_table.ref, idx)}
            triple -> {[triple], iterate(mem_table.ref, idx)}
          end

        idx ->
          {[], iterate(mem_table.ref, idx)}
      end,
      fn _ -> :ok end
    )
  end

  defp maybe_rotate(mem_table) do
    if size(mem_table.ref) > mem_table.mem_limit do
      rotate(mem_table)
    else
      {:ok, mem_table}
    end
  end

  defp rotate(mem_table) do
    with :ok <- FileIO.close(mem_table.io),
         new_id = mem_table.filer.(),
         {:ok, io} <- FileIO.open(new_id, write?: true) do
      ref = new_table()
      {:ok, mem_table, %{mem_table | id: new_id, io: io, ref: ref}}
    end
  end

  defp new_table() do
    :ets.new(:mem_table, [:ordered_set])
  end

  defp insert_commits(commits, ref) do
    commits
    |> Enum.reduce(-1, fn {key, seq, val}, _acc ->
      insert(ref, key, seq, val)
      seq
    end)
  end

  defp insert(ref, key, seq, value) do
    :ets.insert(ref, {{key, -seq}, value})
    :ok
  end

  defp get(ref, key, seq) do
    case :ets.lookup(ref, {key, -seq}) do
      [] -> nil
      [{_, value}] -> {key, seq, value}
    end
  end

  defp search_table(ref, key, seq) do
    case :ets.next(ref, {key, -seq}) do
      {k, s} when key == k ->
        [{_, value}] = :ets.lookup(ref, {k, s})
        {key, abs(s), value}

      _ ->
        nil
    end
  end

  defp table_has_key?(ref, key) do
    case :ets.prev(ref, {key, 1}) do
      {k, _} when k == key -> true
      _ -> false
    end
  end

  defp iterate(ref) do
    idx = :ets.first(ref)
    handle_iteration(ref, idx)
  end

  defp iterate(ref, {key, seq}) do
    idx = :ets.next(ref, {key, -seq})
    handle_iteration(ref, idx)
  end

  defp iterate(ref, idx) do
    idx = :ets.next(ref, idx)
    handle_iteration(ref, idx)
  end

  defp handle_iteration(_ref, :"$end_of_table"), do: :end_of_iteration
  defp handle_iteration(_ref, {key, seq}), do: {key, abs(seq)}
  defp handle_iteration(ref, idx), do: iterate(ref, idx)

  defp size(ref) do
    :ets.info(ref, :memory) * :erlang.system_info(:wordsize)
  end
end
