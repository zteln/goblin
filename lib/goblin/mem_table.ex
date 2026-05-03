defmodule Goblin.MemTable do
  @moduledoc false

  alias Goblin.FileIO

  defstruct [
    :io,
    :path,
    :ref,
    :max_sequence
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

      {:ok, %__MODULE__{io: io, path: path, ref: ref, max_sequence: max_seq}}
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

      {:ok, %{mem_table | max_sequence: max_seq}}
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

  defp search(ref, key, seq) do
    case :ets.next(ref, {key, -seq}) do
      {k, s} when key == k ->
        [{_, value}] = :ets.lookup(ref, {k, s})
        {key, abs(s), value}

      _ ->
        nil
    end
  end

  defp has_key?(ref, key) do
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
end
