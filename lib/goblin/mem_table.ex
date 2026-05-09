defmodule Goblin.MemTable do
  @moduledoc false

  alias Goblin.FileIO

  defstruct [
    :id,
    :io,
    :ref,
    :max_sequence,
    :mem_limit
  ]

  def new(path, opts) do
    with {:ok, io} <- FileIO.open(path, write?: Keyword.get(opts, :write?, true)) do
      ref = new_table()

      max_seq =
        FileIO.stream!(io, truncate?: true)
        |> Enum.reduce(-1, fn {key, seq, val}, acc ->
          insert(ref, key, seq, val)
          max(acc, seq)
        end)

      {:ok,
       %__MODULE__{
         io: io,
         id: path,
         ref: ref,
         max_sequence: max_seq,
         mem_limit: opts[:mem_limit]
       }}
    end
  end

  def append(mem_table, commits) do
    with {:ok, _size} <- FileIO.append(mem_table.io, commits),
         :ok <- FileIO.sync(mem_table.io) do
      max_seq =
        Enum.reduce(commits, mem_table.max_sequence, fn {key, seq, val}, _acc ->
          insert(mem_table.ref, key, seq, val)
          seq
        end)

      mem_table = %{mem_table | max_sequence: max_seq}
      maybe_rotate(mem_table)
    end
  end

  def has_key?(mem_table, key) do
    table_has_key?(mem_table.ref, key)
  end

  def search(mem_table, keys, seq) do
    Enum.flat_map(keys, fn key ->
      case search_table(mem_table.ref, key, seq) do
        nil -> []
        triple -> [triple]
      end
    end)
  end

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
         # increment path
         {:ok, io} <- FileIO.open(mem_table.id, write?: true) do
      ref = new_table()
      {:ok, mem_table, %{mem_table | io: io, ref: ref}}
    end
  end

  defp new_table() do
    :ets.new(:mem_table, [:ordered_set])
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
