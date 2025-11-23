defmodule Goblin.Iterator do
  @moduledoc false
  @type iterator :: {term(), (term() -> {Goblin.triple(), term()} | :ok)}
  @typep cursor :: {iterator(), Goblin.triple() | nil}

  @spec init(term(), (term() -> {Goblin.triple(), term()})) :: cursor()
  def init(state, next) do
    {{state, next}, nil}
  end

  @spec iterate(iterator) :: {term(), iterator()} | :ok
  def iterate({state, next}) do
    case next.(state) do
      :ok -> :ok
      {out, state} -> {out, {state, next}}
    end
  end

  @spec k_merge([cursor()], keyword()) :: {[Goblin.triple()], [cursor()]} | {:halt, :ok}
  def k_merge(cursors, opts) do
    filter_tombstones = Keyword.get(opts, :filter_tombstones, true)
    min = opts[:min]
    max = opts[:max]

    cursors =
      cursors
      |> Enum.flat_map(&skip/1)
      |> Enum.sort_by(fn {_, {key, seq, _}} -> {key, -seq} end)

    case cursors do
      [] ->
        {:halt, :ok}

      [{_, {smallest_key, _, _}} | _] when not is_nil(max) and smallest_key > max ->
        {:halt, :ok}

      [{_, {smallest_key, _, _}} | _] when not is_nil(min) and smallest_key < min ->
        {[], Enum.flat_map(cursors, &jump(&1, smallest_key))}

      [{_, {smallest_key, _, :"$goblin_tombstone"}} | _] when filter_tombstones ->
        {[], Enum.flat_map(cursors, &jump(&1, smallest_key))}

      [{_, {smallest_key, _, _} = next} | _] ->
        {[next], Enum.flat_map(cursors, &jump(&1, smallest_key))}
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
