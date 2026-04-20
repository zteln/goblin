defmodule Goblin.Tx do
  @moduledoc """
  Module for reading and writing within a transaction.

  Used inside `Goblin.transaction/2` (read-write) and `Goblin.read/2` (read-only).

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)

        tx
        |> Goblin.Tx.put(:counter, counter + 1)
        |> Goblin.Tx.commit()
      end)

      Goblin.read(db, fn tx ->
        Goblin.Tx.get(tx, :alice)
      end)
  """

  alias Goblin.{Broker, Mem, Disk, Iterator}

  defstruct [
    :mode,
    :sequence,
    :tx_id,
    :broker,
    :max_level_key,
    writes: []
  ]

  @type t :: %__MODULE__{}

  @doc """
  Writes a key-value pair within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to store
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the key under

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.put(tx, :alice, "Alice")
  """
  @spec put(t(), Goblin.db_key(), Goblin.db_value(), keyword()) :: t()
  def put(tx, key, value, opts \\ [])

  def put(%{mode: :read}, _key, _value, _opts),
    do: raise("Operation not allowed during read")

  def put(tx, key, value, opts) do
    key = tag_key(key, opts[:tag])
    write = {key, tx.sequence, value}
    %{tx | sequence: tx.sequence + 1, writes: [write | tx.writes]}
  end

  @doc """
  Writes multiple key-value pairs within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `pairs` - A list of `{key, value}` tuples
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.put_multi(tx, [{:alice, "Alice"}, {:bob, "Bob"}])
  """
  @spec put_multi(t(), list({Goblin.db_key(), Goblin.db_value()}), keyword()) :: t()
  def put_multi(tx, pairs, opts \\ [])

  def put_multi(%{mode: :read}, _pairs, _opts),
    do: raise("Operation not allowed during read")

  def put_multi(tx, pairs, opts) do
    Enum.reduce(pairs, tx, fn {key, value}, acc ->
      key = tag_key(key, opts[:tag])
      write = {key, acc.sequence, value}
      %{acc | sequence: acc.sequence + 1, writes: [write | acc.writes]}
    end)
  end

  @doc """
  Removes a key within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.remove(tx, :alice)
  """
  @spec remove(t(), Goblin.db_key(), keyword()) :: t()
  def remove(tx, key, opts \\ [])
  def remove(%{mode: :read}, _key, _opts), do: raise("Operation not allowed during read")

  def remove(tx, key, opts) do
    key = tag_key(key, opts[:tag])
    write = {key, tx.sequence, :"$goblin_tombstone"}
    %{tx | sequence: tx.sequence + 1, writes: [write | tx.writes]}
  end

  @doc """
  Removes multiple keys within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `keys` - A list of keys to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.remove_multi(tx, [:alice, :bob])
  """
  @spec remove_multi(t(), list(Goblin.db_key()), keyword()) :: t()
  def remove_multi(tx, keys, opts \\ [])
  def remove_multi(%{mode: :read}, _keys, _opts), do: raise("Operation not allowed during read")

  def remove_multi(tx, keys, opts) do
    Enum.reduce(keys, tx, fn key, acc ->
      key = tag_key(key, opts[:tag])
      write = {key, acc.sequence, :"$goblin_tombstone"}
      %{acc | sequence: acc.sequence + 1, writes: [write | acc.writes]}
    end)
  end

  @doc """
  Retrieves a value within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under
    - `:default` - Value to return if `key` is not found (default: `nil`)

  ## Returns

  - The value associated with the key, or `default` if not found

  ## Examples

      Goblin.Tx.get(tx, :alice)
      # => "Alice"

      Goblin.Tx.get(tx, :nonexistent, default: :not_found)
      # => :not_found
  """
  @spec get(t(), Goblin.db_key(), keyword()) :: Goblin.db_value()
  def get(tx, key, opts \\ []) do
    key = tag_key(key, opts[:tag])
    tx_table = Enum.sort_by(tx.writes, fn {key, seq, _val} -> {key, -seq} end)

    tables = fn level_key ->
      [
        tx_table
        | Broker.filter_tables(
            tx.broker,
            tx.tx_id,
            level_key: level_key,
            filter: &table_has_key?(&1, key)
          )
      ]
    end

    search_opts = [
      sequence: tx.sequence,
      max_level_key: tx.max_level_key,
      tag: opts[:tag]
    ]

    case search(tables, [key], search_opts) do
      [] -> opts[:default]
      [{_key, value}] -> value
    end
  end

  @doc """
  Retrieves values for multiple keys within a transaction.

  Keys not found are excluded from the result.

  ## Parameters

  - `tx` - The transaction struct
  - `keys` - A list of keys to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A list of `{key, value}` tuples for keys found, in unspecified order

  ## Examples

      [{:alice, "Alice"}, {:bob, "Bob"}] = Goblin.Tx.get_multi(tx, [:alice, :bob])
  """
  @spec get_multi(t(), list(Goblin.db_key()), keyword()) ::
          list({Goblin.db_key(), Goblin.db_value()})
  def get_multi(tx, keys, opts \\ []) do
    keys = Enum.map(keys, &tag_key(&1, opts[:tag]))
    tx_table = Enum.sort_by(tx.writes, fn {key, seq, _val} -> {key, -seq} end)

    tables = fn level_key ->
      [
        tx_table
        | Broker.filter_tables(
            tx.broker,
            tx.tx_id,
            level_key: level_key,
            filter: fn table ->
              Enum.any?(keys, &table_has_key?(table, &1))
            end
          )
      ]
    end

    opts = [
      sequence: tx.sequence,
      max_level_key: tx.max_level_key,
      tag: opts[:tag]
    ]

    search(tables, keys, opts)
  end

  @doc """
  Pipeline-friendly helper function to commit the transaction.

  ## Parameters

  - `tx` - The transaction to commit
  - `reply` - The reply after committing (default: `:ok`)

  ## Returns

  - The commit tuple, i.e. `{:commit, tx, reply}`.

  ## Examples

      tx
      |> Goblin.Tx.put(:alice, "Alice")
      |> Goblin.Tx.commit()
  """
  @spec commit(t(), any()) :: {:commit, t(), any()}
  def commit(tx, reply \\ :ok) do
    {:commit, tx, reply}
  end

  @doc """
  Pipeline-friendly helper function to abort the transaction.

  ## Parameters

  - `tx` - The transaction to abort

  ## Returns

  - The abort atom, i.e. `:abort`.

  ## Examples

      tx
      |> Goblin.Tx.put(:alice, "Alice")
      |> Goblin.Tx.abort()
  """
  @spec abort(t()) :: :abort
  def abort(_tx), do: :abort

  @doc false
  @spec scan((-> {non_neg_integer(), list(term())}), keyword()) ::
          Enumerable.t({term(), term()})
  def scan(seq_and_tables, opts) do
    {min, max} = tag_bounds(opts[:min], opts[:max], opts[:tag])

    opts =
      opts
      |> Keyword.put(:min, min)
      |> Keyword.put(:max, max)

    seq_and_tables
    |> scan_levels(opts)
    |> Stream.flat_map(fn triple ->
      case filter_triple_by_tag(triple, opts[:tag]) do
        nil -> []
        pair -> [pair]
      end
    end)
  end

  defp search(tables, keys, opts) do
    search_levels(tables, MapSet.new(keys), opts[:sequence], -1, opts[:max_level_key])
    |> Enum.map(&untag_pair/1)
  end

  defp search_levels(tables, keys, seq, lk, max_lk, acc \\ [])

  defp search_levels(_tables, _keys, _seq, lk, max_lk, acc)
       when lk > max_lk,
       do: acc

  defp search_levels(tables, keys, seq, lk, max_lk, acc) do
    reduced_keys =
      keys
      |> Enum.sort(:desc)
      |> Enum.reduce([], fn
        key1, [key2 | _] = acc when key1 == key2 -> acc
        key, acc -> [key | acc]
      end)

    {acc, keys} =
      Iterator.k_merge(
        fn ->
          tables.(lk)
          |> Enum.map(&table_search(&1, reduced_keys, seq))
        end,
        filter_tombstones?: false
      )
      |> Enum.reduce({acc, keys}, fn
        {key, _seq, :"$goblin_tombstone"}, {acc, keys} ->
          {acc, MapSet.delete(keys, key)}

        {key, _seq, val}, {acc, keys} ->
          {[{key, val} | acc], MapSet.delete(keys, key)}
      end)

    case MapSet.size(keys) do
      0 -> acc
      _ -> search_levels(tables, keys, seq, lk + 1, max_lk, acc)
    end
  end

  defp scan_levels(seq_and_tables, opts) do
    min = opts[:min]
    max = opts[:max]

    Iterator.k_merge(
      fn ->
        {seq, tables} = seq_and_tables.()
        Enum.map(tables, &table_stream(&1, min, max, seq))
      end,
      after: opts[:after],
      min: min,
      max: max
    )
  end

  defp table_has_key?(%Mem{} = table, key), do: Mem.has_key?(table, key)
  defp table_has_key?(%Disk.Table{} = table, key), do: Disk.has_key?(table, key)

  defp table_has_key?(table, key) when is_list(table),
    do: Enum.any?(table, fn {k, _, _} -> k == key end)

  defp table_search(%Mem{} = table, keys, seq), do: Mem.search(table, keys, seq)
  defp table_search(%Disk.Table{} = table, keys, seq), do: Disk.search(table, keys, seq)

  defp table_search(table, keys, seq) when is_list(table),
    do: Enum.filter(table, fn {k, s, _} -> s < seq and k in keys end)

  defp table_stream(%Mem{} = table, _min, _max, seq), do: Mem.stream(table, seq)
  defp table_stream(%Disk.Table{} = table, min, max, seq), do: Disk.stream(table, min, max, seq)

  defp table_stream(table, min, max, seq) when is_list(table) do
    cond do
      is_nil(min) and is_nil(max) -> Enum.filter(table, fn {_k, s, _v} -> s < seq end)
      is_nil(max) -> Enum.filter(table, fn {k, s, _v} -> min <= k and s < seq end)
      is_nil(min) -> Enum.filter(table, fn {k, s, _v} -> k <= max and s < seq end)
      true -> Enum.filter(table, fn {k, s, _v} -> min <= k and k <= max and s < seq end)
    end
  end

  defp tag_key(key, nil), do: key
  defp tag_key(key, tag), do: {:"$goblin_tag", tag, key}

  defp untag_pair({{:"$goblin_tag", _tag, key}, val}), do: {key, val}
  defp untag_pair(pair), do: pair

  defp tag_bounds(min, max, nil), do: {min, max}
  defp tag_bounds(nil, nil, _tag), do: {nil, nil}
  defp tag_bounds(min, nil, tag), do: {{:"$goblin_tag", tag, min}, nil}
  defp tag_bounds(nil, max, tag), do: {nil, {:"$goblin_tag", tag, max}}
  defp tag_bounds(min, max, tag), do: {{:"$goblin_tag", tag, min}, {:"$goblin_tag", tag, max}}

  defp filter_triple_by_tag({{:"$goblin_tag", _tag, _key}, _seq, _val}, nil), do: nil
  defp filter_triple_by_tag({{:"$goblin_tag", tag, key}, _seq, val}, tag), do: {key, val}
  defp filter_triple_by_tag({key, _seq, val}, nil), do: {key, val}
  defp filter_triple_by_tag(_triple, _tag), do: nil
end
