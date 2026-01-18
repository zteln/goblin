defprotocol Goblin.Tx do
  @moduledoc """
  Transaction helpers for working with Goblin database transactions.

  ## Usage

      Goblin.transaction(db, fn tx ->
        user = Goblin.Tx.get(tx, :user_123)
        
        if user do
          updated = Map.update!(user, :login_count, &(&1 + 1))
          tx = Goblin.Tx.put(tx, :user_123, updated)
          {:commit, tx, :ok}
        else
          :abort
        end
      end)

      Goblin.read(db, fn tx -> 
        key1 = Goblin.Tx.get(tx, :start, default: 0)
        key2 = Goblin.Tx.get(tx, key1)
        {key1, key2}
      end)
  """

  @type t :: t()
  @type return :: {:commit, Goblin.Tx.t(), term()} | :abort

  @doc """
  Writes a key-value pair within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to store
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Any Elixir term to use as the tag

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.put(tx, :user, %{name: "Alice"})
  """
  @spec put(t(), Goblin.db_key(), Goblin.db_value(), keyword()) :: t()
  def put(tx, key, value, opts \\ [])

  @doc """
  Writes key-value pairs within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `pairs` - The key-value pairs
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Any Elixir term to use as the tag

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.put_multi(tx, [user1: %{name: "Alice"}, user2: %{name: "Bob"}])
  """
  @spec put_multi(t(), [{Goblin.db_key(), Goblin.db_value()}]) :: t()
  def put_multi(tx, pairs, opts \\ [])

  @doc """
  Removes a key within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag associated with the key

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.remove(tx, :user)
  """
  @spec remove(t(), Goblin.db_key(), keyword()) :: t()
  def remove(tx, key, opts \\ [])

  @doc """
  Removes keys within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `keys` - The list of keys to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag associated with the keys

  ## Returns

  - Updated transaction struct

  ## Examples

      tx = Goblin.Tx.remove_multi(tx, [:user1, :user2])
  """
  @spec remove_multi(t(), [Goblin.db_key()], keyword()) :: t()
  def remove_multi(tx, keys, opts \\ [])

  @doc """
  Retrieves a value within a transaction.

  ## Parameters

  - `tx` - The transaction struct
  - `key` - The key to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag associated with the keys
    - `:default` - Value to return if `key` is not found (default: `nil`)

  ## Returns

  - The value associated with the key, or `default` if not found

  ## Examples

      counter = Goblin.Tx.get(tx, :counter, default: 0)
  """
  @spec get(t(), Goblin.db_key(), keyword()) :: Goblin.db_value()
  def get(tx, key, opts \\ [])

  @doc """
  Retrieves key-value pairs associated with the provided list of keys.

  ## Parameters

  - `tx` - The transaction struct
  - `keys` - A list of keys to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag associated with the keys

  ## Returns

  - Key-value pairs

  ## Examples

      [user1: %{name: "Alice"}, user2: %{name: "Bob"}] = Goblin.Tx.get_multi(tx, [:user1, :user2])
  """
  @spec get_multi(t(), [Goblin.db_key()], keyword()) :: [{Goblin.db_key(), Goblin.db_value()}]
  def get_multi(tx, keys, opts \\ [])

  @doc """
  Retrieves a stream over key-value pairs sorted in ascending order by key.

  ## Parameters

  - `tx` - The transaction struct
  - `opts` - A keyword list with two options:
    - `:min` - The minimum key to range over
    - `:max` - The maximum key to range over
    - `:tag` - Tag to include, `:all` returns a stream over all data (tagged as well as non-tagged data)

  ## Examples

      [user1: %{name: "Alice"}, user2: %{name: "Bob"}] = Goblin.Tx.select(tx) |> Enum.to_list()

      [user1: %{name: "Alice"}] = Goblin.Tx.select(tx, max: :user1) |> Enum.to_list()

      [user2: %{name: "Bob"}] = Goblin.Tx.select(tx, min: :user2) |> Enum.to_list()

      [user1: %{name: "Alice"}, user2: %{name: "Bob"}] = Goblin.Tx.select(tx, min: :user1, max: :user2) |> Enum.to_list()
  """
  @spec select(t(), keyword()) :: Enumerable.t({Goblin.db_key(), Goblin.db_value()})
  def select(tx, opts \\ [])
end
