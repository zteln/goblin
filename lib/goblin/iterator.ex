defprotocol Goblin.Iterable do
  @moduledoc false
  @type t :: t()

  @spec init(t()) :: t()
  def init(iter)

  @spec deinit(t()) :: :ok
  def deinit(iter)

  @spec next(t()) :: t() | :ok
  def next(iter)
end

defmodule Goblin.Iterator do
  @moduledoc false

  @spec linear_stream(Goblin.Iterable.t()) :: Enumerable.t(Goblin.triple())
  def linear_stream(iterator) do
    Stream.resource(
      fn -> Goblin.Iterable.init(iterator) end,
      fn iterator ->
        case iterate(iterator) do
          :ok -> {:halt, nil}
          {triple, iterator} -> {[triple], iterator}
        end
      end,
      fn _ -> :ok end
    )
  end

  @spec k_merge_stream([Goblin.Iterable.t()], keyword()) :: Enumerable.t(Goblin.triple())
  def k_merge_stream(iterators, opts \\ []) do
    Stream.resource(
      fn ->
        Enum.flat_map(iterators, fn iterator ->
          iterator = Goblin.Iterable.init(iterator)

          case iterate(iterator) do
            :ok -> []
            {triple, iterator} -> [{iterator, triple}]
          end
        end)
      end,
      &k_merge(&1, opts),
      fn cursors ->
        Enum.each(cursors, fn
          {nil, _} -> :ok
          {iterator, _} -> Goblin.Iterable.deinit(iterator)
          _ -> :ok
        end)
      end
    )
  end

  defp k_merge(cursors, opts) do
    filter_tombstones = Keyword.get(opts, :filter_tombstones, true)
    min = opts[:min]
    max = opts[:max]

    cursors = Enum.sort_by(cursors, fn {_, {key, seq, _}} -> {key, -seq} end)

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
      :ok -> Goblin.Iterable.deinit(iterator)
      {out, iterator} -> {out, iterator}
    end
  end

  defp jump({nil, {key1, _, _}}, key2) when key1 == key2, do: []
  defp jump({nil, triple}, _), do: [{nil, triple}]

  defp jump({iterator, {key1, _, _}}, key2) when key1 == key2 do
    case iterate(iterator) do
      :ok -> []
      {{key, _, _} = triple, iterator} when key == key1 -> jump({iterator, triple}, key2)
      {next, iterator} -> [{iterator, next}]
    end
  end

  defp jump(cursor, _key), do: [cursor]
end
