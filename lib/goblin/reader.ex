defmodule Goblin.Reader do
  @moduledoc false
  alias Goblin.SSTs
  alias Goblin.Writer
  alias Goblin.Store

  @spec get(Goblin.db_key(), Goblin.Writer.writer(), Goblin.Store.store(), Goblin.seq_no() | nil) ::
          {Goblin.seq_no(), Goblin.db_value()} | :not_found
  def get(key, writer, store, max_seq \\ nil) do
    case try_writer(writer, key, max_seq) do
      {:value, _seq, :tombstone} ->
        :not_found

      {:value, seq, value} ->
        {seq, value}

      :not_found ->
        try_store(store, key)
    end
  end

  @spec get_multi([Goblin.db_key()], Goblin.Writer.writer(), Goblin.Store.store()) :: [
          Goblin.triple()
        ]
  def get_multi(keys, writer, store) do
    {found, not_found} = try_writer(writer, keys)
    result = found ++ try_store(store, not_found)

    Enum.filter(result, fn
      {_seq, _key, :tombstone} -> false
      :not_found -> false
      _ -> true
    end)
  end

  @spec select(
          Goblin.db_key() | nil,
          Goblin.db_key() | nil,
          Goblin.Writer.writer(),
          Goblin.Store.store()
        ) :: Enumerable.t(Goblin.pair())
  def select(min, max, writer, store) do
    Stream.resource(
      fn ->
        mem_iterators = Writer.iterators(writer, min, max)
        sst_iterators = Store.iterators(store, min, max)

        iterators =
          Enum.flat_map([mem_iterators | sst_iterators], &init_iterator/1)
          |> advance_until(min)

        first = take_smallest(iterators)
        iterators = advance(iterators, first)
        {iterators, first}
      end,
      fn
        {_, nil} ->
          {:halt, :ok}

        {_iterators, {prev_min, _, _}} when not is_nil(max) and prev_min > max ->
          {:halt, :ok}

        {iterators, {_, _, :tombstone}} ->
          next = take_smallest(iterators)
          iterators = advance(iterators, next)
          {[], {iterators, next}}

        {[], {last_min, _, last_val}} ->
          {[{last_min, last_val}], {[], nil}}

        {_iterators, {prev_min, _, prev_val}} when not is_nil(max) and prev_min == max ->
          {[{prev_min, prev_val}], {[], nil}}

        {iterators, {prev_min, _, prev_val}} ->
          next = take_smallest(iterators)
          iterators = advance(iterators, next)
          {[{prev_min, prev_val}], {iterators, next}}
      end,
      fn _ -> :ok end
    )
  end

  defp init_iterator({iter, iter_f}) do
    case iter_f.(iter) do
      :ok -> []
      {next, iter} -> [{next, iter, iter_f}]
    end
  end

  defp take_smallest(iterators) do
    iterators
    |> Enum.map(&elem(&1, 0))
    |> Enum.reduce(nil, &choose_smallest/2)
  end

  defp choose_smallest(next, nil), do: next

  defp choose_smallest({k1, s1, _} = next, {k2, s2, _} = prev) do
    cond do
      k1 == k2 and s1 > s2 -> next
      k1 < k2 -> next
      true -> prev
    end
  end

  defp advance_until(iterators, target, acc \\ [])
  defp advance_until([], _target, acc), do: Enum.reverse(acc)
  defp advance_until(iterators, nil, _acc), do: iterators

  defp advance_until([{{k, _, _}, iter, iter_f} = iterator | iterators], target, acc) do
    if k >= target do
      advance_until(iterators, target, [iterator | acc])
    else
      case iter_f.(iter) do
        :ok -> advance_until(iterators, target, acc)
        {next, iter} -> advance_until([{next, iter, iter_f} | iterators], target, acc)
      end
    end
  end

  defp advance(iterators, min, acc \\ [])
  defp advance([], _, acc), do: Enum.reverse(acc)
  defp advance(iterators, nil, _), do: iterators
  defp advance(iterators, {min, _, _}, acc), do: advance(iterators, min, acc)

  defp advance([{{k, _, _}, iter, iter_f} = iterator | iterators], min, acc) do
    if k <= min do
      case iter_f.(iter) do
        :ok -> advance(iterators, min, acc)
        {next, iter} -> advance([{next, iter, iter_f} | iterators], min, acc)
      end
    else
      advance(iterators, min, [iterator | acc])
    end
  end

  defp try_writer(writer, key, max_seq \\ nil)
  defp try_writer(writer, keys, _max_seq) when is_list(keys), do: Writer.get_multi(writer, keys)
  defp try_writer(writer, key, max_seq), do: Writer.get(writer, key, max_seq)

  defp try_store(store, keys, max_seq \\ nil)

  defp try_store(store, keys, max_seq) when is_list(keys) do
    Store.get_multi(store, keys)
    |> Task.async_stream(fn {key, ssts} ->
      case read_ssts(key, ssts, max_seq) do
        [] -> :not_found
        [{:value, _seq, :tombstone}] -> :not_found
        [{:value, seq, value}] -> {key, seq, value}
      end
    end)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Enum.to_list()
  end

  defp try_store(store, key, max_seq) do
    {_key, ssts} = Store.get(store, key)

    case read_ssts(key, ssts, max_seq) do
      [] -> :not_found
      [{:value, _seq, :tombstone}] -> :not_found
      [{:value, seq, value}] -> {seq, value}
    end
  end

  defp read_ssts(key, ssts, max_seq) do
    Task.async_stream(ssts, fn sst ->
      SSTs.find(sst.file, key)
    end)
    |> Stream.map(fn {:ok, res} -> res end)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Stream.filter(fn
      {:value, _seq, _value} when is_nil(max_seq) -> true
      {:value, seq, _value} when seq <= max_seq -> true
      _ -> false
    end)
    |> Enum.sort_by(&elem(&1, 1), :desc)
    |> Enum.take(1)
  end
end
