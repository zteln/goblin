defmodule Goblin.Tx do
  @moduledoc """
  Transaction helpers for working with Goblin database transactions.

  ## Usage

      alias Goblin.Tx

      Goblin.transaction(db, fn tx ->
        user = Tx.get(tx, :user_123)
        
        if user do
          updated = Map.update!(user, :login_count, &(&1 + 1))
          tx = Tx.put(tx, :user_123, updated)
          {:commit, tx, :ok}
        else
          :cancel
        end
      end)

  ## Snapshot isolation

  Transactions operate on a snapshot of the database at the time the
  transaction starts. Reads within a transaction see a consistent view
  of the data, even if other transactions commit changes.

  If two transactions modify the same keys, the second transaction to
  commit will fail with `{:error, :in_conflict}`.

  ## Return values

  Transaction functions must return either:

  - `{:commit, tx, result}` - Commits the transaction and returns `result`
  - `:cancel` - Cancels the transaction and returns `:ok`
  """

  @opaque t :: Goblin.Writer.Transaction.t()

  @doc """
  Writes a key-value pair within a transaction.

  The write is buffered in the transaction and only becomes visible to other
  transactions after a successful commit.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to store

  ## Returns

  - Updated transaction struct

  ## Examples

      Goblin.transaction(db, fn tx ->
        tx = Goblin.Tx.put(tx, :counter, 42)
        {:commit, tx, :ok}
      end)
  """
  defdelegate put(tx, key, value), to: Goblin.Writer.Transaction

  @doc """
  Removes a key within a transaction.

  The removal is buffered in the transaction and only takes effect after
  a successful commit.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to remove

  ## Returns

  - Updated transaction struct

  ## Examples

      Goblin.transaction(db, fn tx ->
        tx = Goblin.Tx.remove(tx, :old_key)
        {:commit, tx, :ok}
      end)
  """
  defdelegate remove(tx, key), to: Goblin.Writer.Transaction

  @doc """
  Retrieves a value within a transaction.

  Reads from the transaction's snapshot, including any uncommitted writes
  made within the same transaction. Returns `nil` if the key is not found.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to look up

  ## Returns

  - The value associated with the key, or `nil` if not found

  ## Examples

      Goblin.transaction(db, fn tx ->
        {val, tx} = Goblin.Tx.get(tx, :counter) || 0
        tx = Goblin.Tx.put(tx, :counter, value + 1)
        {:commit, tx, value + 1}
      end)
  """
  defdelegate get(tx, key, default \\ nil), to: Goblin.Writer.Transaction
end
