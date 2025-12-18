defprotocol Goblin.Iterable do
  @moduledoc false
  @type t :: t()
  @spec init(t()) :: t()
  def init(iter)
  @spec next(t()) :: t()
  def next(iter)
  @spec close(t()) :: t()
  def close(iter)
end

defmodule Goblin.Iterator do
  @moduledoc false

  @spec k_merge_stream([Goblin.Iterable.t()], keyword()) :: Enumerable.t(Goblin.triple())
  def k_merge_stream(iterators, opts \\ []) do
    Stream.resource(
      fn -> Enum.map(iterators, &{Goblin.Iterable.init(&1), nil}) end,
      &k_merge(&1, opts),
      &Enum.each(&1, fn
        {iterator, _} -> Goblin.Iterable.close(iterator)
        _ -> :ok
      end)
    )
  end

  defp k_merge(cursors, opts) do
    filter_tombstones = Keyword.get(opts, :filter_tombstones, true)
    min = opts[:min]
    max = opts[:max]

    cursors =
      cursors
      |> Enum.flat_map(&skip/1)
      |> Enum.sort_by(fn {_, {key, seq, _}} -> {key, -seq} end)

    case cursors do
      [] ->
        {:halt, []}

      [{_, {smallest_key, _, _}} | _] when not is_nil(max) and smallest_key > max ->
        {:halt, cursors}

      [{_, {smallest_key, _, _}} | _] when not is_nil(min) and smallest_key < min ->
        {[], Enum.flat_map(cursors, &jump(&1, smallest_key))}

      [{_, {smallest_key, _, :"$goblin_tombstone"}} | _] when filter_tombstones ->
        {[], Enum.flat_map(cursors, &jump(&1, smallest_key))}

      [{_, {smallest_key, _, _} = next} | _] ->
        {[next], Enum.flat_map(cursors, &jump(&1, smallest_key))}
    end
  end

  defp iterate(iterator) do
    case Goblin.Iterable.next(iterator) do
      :ok -> Goblin.Iterable.close(iterator)
      {out, iterator} -> {out, iterator}
    end
  end

  defp skip({nil, data}), do: [{nil, data}]

  defp skip({iterator, nil}) do
    case iterate(iterator) do
      :ok -> []
      {data, iterator} -> skip({iterator, data})
    end
  end

  defp skip({iterator, data}) do
    {key, _, _} = data

    case iterate(iterator) do
      :ok -> [{nil, data}]
      {{^key, _, _} = data, iterator} -> skip({iterator, data})
      _ -> [{iterator, data}]
    end
  end

  defp jump({nil, {key, _, _}}, key), do: []
  defp jump({nil, data}, _), do: [{nil, data}]

  defp jump({iterator, {key, _, _}}, key) do
    case iterate(iterator) do
      :ok -> []
      {next, iterator} -> [{iterator, next}]
    end
  end

  defp jump(cursor, _key), do: [cursor]
end
