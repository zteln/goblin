defprotocol Goblin.Tx do
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

  ## Return values

  Write transaction functions must return either:

  - `{:commit, tx, result}` - Commits the transaction and returns `result`
  - `:cancel` - Cancels the transaction and returns `:ok`

  Returning anything else causes the write transaction to raise.

  Read transactions return the last evaluation.
  """

  @type t :: t()
  @type return :: {:commit, Goblin.Tx.t(), term()} | :cancel | any()

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
  @spec put(t(), Goblin.db_key(), Goblin.db_value()) :: t()
  def put(tx, key, value)

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
  @spec remove(t(), Goblin.db_key()) :: t()
  def remove(tx, key)

  @doc """
  Retrieves a value within a transaction.

  Reads from the transaction's snapshot, including any uncommitted writes
  made within the same transaction. 

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to look up
  - `default` - A default value if the key is not found, defaults to `nil`

  ## Returns

  - The value associated with the key, or `nil` if not found

  ## Examples

      Goblin.transaction(db, fn tx ->
        value = Goblin.Tx.get(tx, :counter, 0)
        tx = Goblin.Tx.put(tx, :counter, value + 1)
        {:commit, tx, value + 1}
      end)
  """
  @spec get(t(), Goblin.db_key(), term()) :: Goblin.db_value()
  def get(tx, key, default \\ nil)
end
