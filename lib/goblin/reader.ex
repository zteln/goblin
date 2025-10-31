defmodule Goblin.Reader do
  @moduledoc false
  alias Goblin.Writer
  alias Goblin.Store

  @spec get(Goblin.db_key(), atom()) ::
          Goblin.db_value() | :not_found
  def get(key, registry) do
    case try_writer(registry, key) do
      {:ok, {:value, _seq, :tombstone}} ->
        :not_found

      {:ok, {:value, seq, value}} ->
        {seq, value}

      :not_found ->
        try_store(registry, key)
    end
  end

  @spec get_multi([Goblin.db_key()], atom()) :: [
          {Goblin.db_key(), Goblin.db_sequence(), Goblin.db_value()} | :not_found
        ]
  def get_multi(keys, registry) do
    {found, not_found} = try_writer(registry, keys)
    found ++ try_store(registry, not_found)
  end

  @spec select(Goblin.db_key() | nil, Goblin.db_key() | nil, atom()) ::
          Enumerable.t()
  def select(min, max, registry) do
    Stream.resource(
      fn ->
        mem_iterators = init_mem_iterators(registry)
        {level_iterators, defer} = init_store_iterators(registry, min, max)
        iterators = advance_until(mem_iterators ++ level_iterators, min)
        first = take_smallest(iterators)
        iterators = advance(iterators, first)
        {iterators, first, defer}
      end,
      fn
        {_, nil, defer} ->
          {:halt, defer}

        {_iterators, {_, prev_min, _}, defer} when not is_nil(max) and prev_min > max ->
          {:halt, defer}

        {iterators, {_, _, :tombstone}, defer} ->
          next = take_smallest(iterators)
          iterators = advance(iterators, next)
          {[], {iterators, next, defer}}

        {[], {_, last_min, last_val}, defer} ->
          {[{last_min, last_val}], {[], nil, defer}}

        {_iterators, {_, prev_min, prev_val}, defer} when not is_nil(max) and prev_min == max ->
          {[{prev_min, prev_val}], {[], nil, defer}}

        {iterators, {_, prev_min, prev_val}, defer} ->
          next = take_smallest(iterators)
          iterators = advance(iterators, next)
          {[{prev_min, prev_val}], {iterators, next, defer}}
      end,
      fn defer ->
        Enum.each(defer, & &1.())
      end
    )
  end

  defp init_mem_iterators(registry) do
    Writer.get_iterators(registry)
    |> Enum.flat_map(fn {start_f, iter_f} ->
      iter = start_f.()

      case iter_f.(iter) do
        :ok -> []
        {next, iter} -> [{next, iter, iter_f}]
      end
    end)
  end

  defp init_store_iterators(registry, min, max) do
    Store.get_iterators(registry, min, max)
    |> Enum.reduce({[], []}, fn {{start_f, iter_f}, unlock_f}, {level_iterators, defer} ->
      iter = start_f.()

      case iter_f.(iter) do
        :ok -> {level_iterators, [unlock_f | defer]}
        {next, iter} -> {[{next, iter, iter_f} | level_iterators], [unlock_f | defer]}
      end
    end)
  end

  defp take_smallest(iterators) do
    iterators
    |> Enum.map(&elem(&1, 0))
    |> Enum.reduce(nil, &choose_smallest/2)
  end

  defp choose_smallest(next, nil), do: next

  defp choose_smallest({p1, k1, _} = next, {p2, k2, _} = prev) do
    cond do
      k1 == k2 and p1 > p2 -> next
      k1 < k2 -> next
      true -> prev
    end
  end

  defp advance_until(iterators, target, acc \\ [])
  defp advance_until([], _target, acc), do: Enum.reverse(acc)
  defp advance_until(iterators, nil, _acc), do: iterators

  defp advance_until([{{_, k, _}, iter, iter_f} = iterator | iterators], target, acc) do
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
  defp advance(iterators, {_, min, _}, acc), do: advance(iterators, min, acc)

  defp advance([{{_, k, _}, iter, iter_f} = iterator | iterators], min, acc) do
    if k <= min do
      case iter_f.(iter) do
        :ok -> advance(iterators, min, acc)
        {next, iter} -> advance([{next, iter, iter_f} | iterators], min, acc)
      end
    else
      advance(iterators, min, [iterator | acc])
    end
  end

  defp try_writer(registry, keys) when is_list(keys), do: Writer.get_multi(registry, keys)
  defp try_writer(registry, key), do: Writer.get(registry, key)

  defp try_store(registry, keys) when is_list(keys) do
    keys_and_ssts = Store.get(registry, keys)

    keys_and_ssts
    |> Task.async_stream(fn {key, ssts} ->
      case async_read_ssts(ssts) do
        :not_found -> :not_found
        {seq, value} -> {key, seq, value}
      end
    end)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Enum.to_list()
  end

  defp try_store(registry, key) do
    [{_key, ssts}] = Store.get(registry, key)
    async_read_ssts(ssts)
  end

  defp async_read_ssts(ssts) do
    result =
      ssts
      |> Stream.map(&elem(&1, 0))
      |> Task.async_stream(& &1.())
      |> Stream.map(fn {:ok, res} -> res end)
      |> Stream.map(fn
        {:error, reason} ->
          raise "Failed to read, reason: #{inspect(reason)}"

        res ->
          res
      end)
      |> Stream.filter(&match?({:ok, _}, &1))
      |> Stream.map(fn {:ok, res} -> res end)
      |> Enum.sort_by(&elem(&1, 1), :desc)
      |> Enum.take(1)

    Enum.each(ssts, &elem(&1, 1).())

    case result do
      [] -> :not_found
      [{:value, _seq, :tombstone}] -> :not_found
      [{:value, seq, value}] -> {seq, value}
    end
  end
end
