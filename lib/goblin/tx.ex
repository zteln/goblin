defmodule Goblin.Tx do
  @moduledoc """
  Transaction helpers for working with Goblin database transactions.

  This module provides a simplified API for transaction operations,
  delegating to `Goblin.Writer.Transaction` internally.

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
  defdelegate put(tx, key, value), to: Goblin.Writer.Transaction
  defdelegate remove(tx, key), to: Goblin.Writer.Transaction
  defdelegate get(tx, key), to: Goblin.Writer.Transaction
end
