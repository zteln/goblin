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

  alias Goblin.MVCC
  alias Goblin.MemTable
  alias Goblin.DiskTable
  alias Goblin.Merge

  defstruct [
    :sqn,
    :ref,
    :mvcc,
    :max_level_key,
    mode: :read,
    commits: []
  ]

  @type t :: %__MODULE__{
          mode: :write | :read,
          sqn: non_neg_integer(),
          ref: reference(),
          mvcc: :ets.table(),
          max_level_key: -1 | non_neg_integer(),
          commits: list({term(), non_neg_integer(), term()})
        }

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
  @spec put(t(), term(), term(), keyword()) :: t()
  def put(tx, key, value, opts \\ [])

  def put(%{mode: :read}, _key, _value, _opts),
    do: raise(ArgumentError, "Operation not allowed during read")

  def put(tx, key, value, opts) do
    tag = Keyword.get(opts, :tag, :"$goblin_nil")
    key = tag_key(key, tag)
    commit = {key, tx.sqn, value}
    %{tx | sqn: tx.sqn + 1, commits: [commit | tx.commits]}
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
  @spec put_multi(t(), list({term(), term()}), keyword()) :: t()
  def put_multi(tx, pairs, opts \\ [])

  def put_multi(%{mode: :read}, _pairs, _opts),
    do: raise(ArgumentError, "Operation not allowed during read")

  def put_multi(tx, pairs, opts) do
    tag = Keyword.get(opts, :tag, :"$goblin_nil")

    Enum.reduce(pairs, tx, fn {key, value}, acc ->
      key = tag_key(key, tag)
      commit = {key, acc.sqn, value}
      %{acc | sqn: acc.sqn + 1, commits: [commit | acc.commits]}
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
  @spec remove(t(), term(), keyword()) :: t()
  def remove(tx, key, opts \\ [])

  def remove(%{mode: :read}, _key, _opts),
    do: raise(ArgumentError, "Operation not allowed during read")

  def remove(tx, key, opts) do
    tag = Keyword.get(opts, :tag, :"$goblin_nil")
    key = tag_key(key, tag)
    commit = {key, tx.sqn, :"$goblin_tombstone"}
    %{tx | sqn: tx.sqn + 1, commits: [commit | tx.commits]}
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
  @spec remove_multi(t(), list(term()), keyword()) :: t()
  def remove_multi(tx, keys, opts \\ [])

  def remove_multi(%{mode: :read}, _keys, _opts),
    do: raise(ArgumentError, "Operation not allowed during read")

  def remove_multi(tx, keys, opts) do
    tag = Keyword.get(opts, :tag, :"$goblin_nil")

    Enum.reduce(keys, tx, fn key, acc ->
      key = tag_key(key, tag)
      commit = {key, acc.sqn, :"$goblin_tombstone"}
      %{acc | sqn: acc.sqn + 1, commits: [commit | acc.commits]}
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
  @spec get(t(), term(), keyword()) :: term()
  def get(tx, key, opts \\ []) do
    case get_multi(tx, [key], opts) do
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
  @spec get_multi(t(), list(term()), keyword()) :: list({term(), term()})
  def get_multi(tx, keys, opts \\ []) do
    tag = Keyword.get(opts, :tag, :"$goblin_nil")
    keys = keys |> Enum.map(&tag_key(&1, tag)) |> :lists.usort()

    {found, _missing} =
      Enum.reduce_while(-2..tx.max_level_key//1, {[], keys}, fn
        _lk, {found, []} ->
          {:halt, {found, []}}

        lk, {found, keys} ->
          hits = search_level(tx, lk, keys)
          {:cont, {hits ++ found, :ordsets.subtract(keys, Enum.map(hits, &elem(&1, 0)))}}
      end)

    for {key, _sqn, val} <- found, val != :"$goblin_tombstone", do: untag_pair({key, val})
  end

  @doc """
  Checks whether a key exists or not within a transaction.

  > #### False positives {: .note}
  >
  > If the key has been flushed to disk, then membership is checked via the disk table's Bloom filters, i.e. it can yield a false positive in some cases.

  ## Parameters
    
  - `tx` - The transaction struct
  - `key` - The key to check membership off
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A boolean indicating if the key exists or not

  ## Examples

    Goblin.Tx.has_key?(tx, :alice)
    # => false
    Goblin.Tx.put(tx, :alice, "Alice")
    Goblin.Tx.has_key?(tx, :alice)
    # => true
  """
  @spec has_key?(t(), term(), keyword()) :: boolean()
  def has_key?(tx, key, opts \\ []) do
    get_multi(tx, [key], opts) != []
  end

  @doc """
  Lazily streams the database inside a transaction, including pending writes.
  The transaction must be enumerated inside the transaction, otherwise `RuntimeError` is raised.

  ## Parameters
    
  - `tx` - The transaction struct
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:min` - Minimum key, inclusive (optional)
    - `:max` - Maximum key, inclusive (optional)
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A lazy stream

  ## Examples

      Goblin.Tx.scan(tx) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.Tx.scan(tx, min: :bob) |> Enum.to_list()
      # => [{:bob, "Bob"}, {:charlie, "Charlie"}]

      Goblin.Tx.scan(tx, min: :alice, max: :bob) |> Enum.to_list()
      # => [{:alice, "Alice"}, {:bob, "Bob"}]
  """
  @spec scan(t(), keyword()) :: Enumerable.t({term(), term()})
  def scan(tx, opts \\ []) do
    min = Keyword.get(opts, :min, :"$goblin_nil")
    max = Keyword.get(opts, :max, :"$goblin_nil")
    tag = Keyword.get(opts, :tag, :"$goblin_nil")
    {min, max} = tag_bounds(min, max, tag)

    Merge.stream(
      fn ->
        if not MVCC.pinned?(tx.mvcc, tx.ref),
          do:
            raise(
              "Goblin.Tx.scan/2 stream was enumerated outside its transaction; " <>
                "consume it inside the read/transaction callback that created it"
            )

        tx_table = Enum.sort_by(tx.commits, fn {key, sqn, _val} -> {key, -sqn} end)

        [tx_table | MVCC.get_all_tables(tx.mvcc, tx.ref)]
        |> Enum.map(&table_stream(&1, min, max, tx.sqn))
      end,
      min: min,
      max: max
    )
    |> Stream.flat_map(fn triple ->
      case filter_triple_by_tag(triple, tag) do
        nil -> []
        pair -> [pair]
      end
    end)
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
  def commit(tx, reply \\ :ok), do: {:commit, tx, reply}

  @doc """
  Pipeline-friendly helper function to abort the transaction.

  ## Parameters

  - `tx` - The transaction to abort
  - `reply` - The reply after aborting (default: `:error`)

  ## Returns

  - The abort tuple, i.e. `{:abort, reply}`.

  ## Examples

      tx
      |> Goblin.Tx.put(:alice, "Alice")
      |> Goblin.Tx.abort()
  """
  @spec abort(t(), any()) :: {:abort, any()}
  def abort(_tx, reply \\ :error), do: {:abort, reply}

  defp search_level(tx, -2, keys) do
    for key <- keys, hit = List.keyfind(tx.commits, key, 0), do: hit
  end

  defp search_level(tx, lk, keys) do
    Merge.stream(
      fn ->
        tx.mvcc
        |> MVCC.get_matching_tables(tx.ref, lk, keys)
        |> Enum.map(&table_search(&1, keys, tx.sqn))
      end,
      filter_tombstones?: false
    )
    |> Enum.to_list()
  end

  defp table_search(%MemTable{} = mt, keys, sqn), do: MemTable.search(mt, keys, sqn)

  defp table_search(%DiskTable{} = dt, keys, sqn) do
    case Enum.filter(keys, &DiskTable.has_key?(dt, &1)) do
      [] -> []
      hits -> DiskTable.search(dt, hits, sqn)
    end
  end

  defp table_stream(%MemTable{} = mt, _min, _max, sqn), do: MemTable.stream(mt, sqn)

  defp table_stream(%DiskTable{} = dt, min, max, sqn) do
    {dt_min, dt_max} = dt.key_range
    min = if min == :"$goblin_nil", do: dt_min, else: min
    max = if max == :"$goblin_nil", do: dt_max, else: max
    DiskTable.stream(dt, min, max, sqn)
  end

  defp table_stream(table, min, max, sqn) when is_list(table) do
    cond do
      min == :"$goblin_nil" and max == :"$goblin_nil" ->
        Enum.filter(table, fn {_k, s, _v} -> s < sqn end)

      max == :"$goblin_nil" ->
        Enum.filter(table, fn {k, s, _v} -> min <= k and s < sqn end)

      min == :"$goblin_nil" ->
        Enum.filter(table, fn {k, s, _v} -> k <= max and s < sqn end)

      true ->
        Enum.filter(table, fn {k, s, _v} -> min <= k and k <= max and s < sqn end)
    end
  end

  defp tag_key(key, :"$goblin_nil"), do: key
  defp tag_key(key, tag), do: {:"$goblin_tag", tag, {key}}

  defp untag_pair({{:"$goblin_tag", _tag, {key}}, val}), do: {key, val}
  defp untag_pair(pair), do: pair

  defp tag_bounds(min, max, :"$goblin_nil"), do: {min, max}

  defp tag_bounds(min, max, tag) do
    lo = if min == :"$goblin_nil", do: {}, else: {min}
    hi = if max == :"$goblin_nil", do: {nil, nil}, else: {max}
    {{:"$goblin_tag", tag, lo}, {:"$goblin_tag", tag, hi}}
  end

  defp filter_triple_by_tag({{:"$goblin_tag", _tag, _key}, _sqn, _val}, :"$goblin_nil"), do: nil
  defp filter_triple_by_tag({{:"$goblin_tag", tag, {key}}, _sqn, val}, tag), do: {key, val}
  defp filter_triple_by_tag({key, _sqn, val}, :"$goblin_nil"), do: {key, val}
  defp filter_triple_by_tag(_triple, _tag), do: nil
end
