defmodule Goblin.Tx do
  @moduledoc """
  Module for reading and writing within a transaction.

  Used inside `Goblin.transaction/2` (read-write) and `Goblin.read/2` (read-only).

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)
        tx = Goblin.Tx.put(tx, :counter, counter + 1)
        {:commit, tx, :ok}
      end)

      Goblin.read(db, fn tx ->
        Goblin.Tx.get(tx, :alice)
      end)
  """

  @type t :: Goblin.Transactionable.t()

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
  @spec put(Goblin.Transactionable.t(), Goblin.db_key(), Goblin.db_value(), keyword()) ::
          Goblin.Transactionable.t()
  defdelegate put(tx, key, value, opts \\ []), to: Goblin.Transactionable

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
  @spec put_multi(
          Goblin.Transactionable.t(),
          list({Goblin.db_key(), Goblin.db_value()}),
          keyword()
        ) :: Goblin.Transactionable.t()
  defdelegate put_multi(tx, pairs, opts \\ []), to: Goblin.Transactionable

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
  @spec remove(Goblin.Transactionable.t(), Goblin.db_key(), keyword()) ::
          Goblin.Transactionable.t()
  defdelegate remove(tx, key, opts \\ []), to: Goblin.Transactionable

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
  @spec remove_multi(Goblin.Transactionable.t(), list(Goblin.db_key()), keyword()) ::
          Goblin.Transactionable.t()
  defdelegate remove_multi(tx, keys, opts \\ []), to: Goblin.Transactionable

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
  @spec get(Goblin.Transactionable.t(), Goblin.db_key(), keyword()) :: Goblin.db_value()
  defdelegate get(tx, key, opts \\ []), to: Goblin.Transactionable

  @doc """
  Retrieves values for multiple keys within a transaction.

  Keys not found are excluded from the result.

  ## Parameters

  - `tx` - The transaction struct
  - `keys` - A list of keys to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A list of `{key, value}` tuples for keys found, sorted by key

  ## Examples

      [{:alice, "Alice"}, {:bob, "Bob"}] = Goblin.Tx.get_multi(tx, [:alice, :bob])
  """
  @spec get_multi(Goblin.Transactionable.t(), list(Goblin.db_key()), keyword()) ::
          list({Goblin.db_key(), Goblin.db_value()})
  defdelegate get_multi(tx, keys, opts \\ []), to: Goblin.Transactionable

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
  @spec commit(Goblin.Transactionable.t(), any()) :: {:commit, Goblin.Transactionable.t(), any()}
  def commit(tx, reply \\ :ok), do: {:commit, tx, reply}

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
  @spec abort(Goblin.Transactionable.t()) :: :abort
  def abort(_tx), do: :abort
end
