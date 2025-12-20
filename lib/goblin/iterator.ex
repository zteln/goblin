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
      fn cursors -> Enum.each(cursors, &k_merge_close/1) end
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

  defp skip({nil, triple}), do: [{nil, triple}]

  defp skip({iterator, nil}) do
    case iterate(iterator) do
      :ok -> []
      {triple, iterator} -> skip({iterator, triple})
    end
  end

  defp skip({iterator, triple}) do
    {key, _, _} = triple

    case iterate(iterator) do
      :ok -> [{nil, triple}]
      {{^key, _, _} = triple, iterator} -> skip({iterator, triple})
      _ -> [{iterator, triple}]
    end
  end

  defp jump({nil, {key, _, _}}, key), do: []
  defp jump({nil, triple}, _), do: [{nil, triple}]

  defp jump({iterator, {key, _, _}}, key) do
    case iterate(iterator) do
      :ok -> []
      {next, iterator} -> [{iterator, next}]
    end
  end

  defp jump(cursor, _key), do: [cursor]

  defp k_merge_close({nil, _}), do: :ok
  defp k_merge_close({iterator, _}), do: Goblin.Iterable.close(iterator)
  defp k_merge_close(_), do: :ok
end
