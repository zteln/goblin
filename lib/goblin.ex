defmodule Goblin do
  @moduledoc """
  A lightweight, embedded, LSM-tree database for Elixir.

  Goblin is a persistent key-value store with ACID transactions, crash
  recovery, and automatic background compaction. It runs inside your
  application's supervision tree.

  ## Starting a database

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        data_dir: "/path/to/db"
      )

  ## Basic operations

      Goblin.put(db, :alice, "Alice")
      Goblin.get(db, :alice)
      # => "Alice"

      Goblin.remove(db, :alice)
      Goblin.get(db, :alice)
      # => nil

  ## Batch operations

      Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}])

      Goblin.get_multi(db, [:alice, :bob])
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

  ## Transactions

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)

        tx
        |> Goblin.Tx.put(:counter, counter + 1)
        |> Goblin.Tx.commit()
      end)
      # => :ok

  See `start_link/1` for configuration options.
  """

  alias Goblin.Server
  alias Goblin.Tx
  alias Goblin.MVCC

  @doc """
  Executes a read-write transaction.

  Transactions are executed serially and are ACID-compliant.
  The provided function receives a transaction struct and must return
  `{:commit, tx, reply}` to commit, or `{:abort, reply}` to abort.

  Calling `transaction` or any of its derivatives (`put`, `put_multi`, `remove`, `remove_multi`) on the same database from within a transaction raises.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `callback` - A function that takes a `Goblin.Tx.t()` and returns a transaction result

  ## Returns

  - `reply` - The reply from `{:commit, tx, reply}` when committed
  - `{:error, :aborted}` - When the transaction is aborted

  ## Examples

      Goblin.transaction(db, fn tx ->
        counter = Goblin.Tx.get(tx, :counter, default: 0)
        tx
        |> Goblin.Tx.put(:counter, counter + 1)
        |> Goblin.Tx.commit()
      end)
      # => :ok

      Goblin.transaction(db, fn tx ->
        tx
        |> Goblin.Tx.abort()
      end)
      # => :error
  """
  @spec transaction(
          Server.t(),
          (Tx.t() -> {:commit, Tx.t(), term()} | {:abort, term()})
        ) :: term()
  def transaction(db, callback) do
    {db, mvcc} = server_info(db)
    tx_ref = make_ref()

    case Server.start_transaction(db, tx_ref) do
      :ok ->
        result =
          try do
            {sqn, max_lk} = MVCC.pin(mvcc, tx_ref)

            %Tx{
              mode: :write,
              mvcc: mvcc,
              ref: tx_ref,
              sqn: sqn,
              max_level_key: max_lk
            }
            |> callback.()
          rescue
            exception ->
              Server.cancel_transaction(db, tx_ref)
              reraise(exception, __STACKTRACE__)
          catch
            :throw, val ->
              Server.cancel_transaction(db, tx_ref)
              throw(val)

            :exit, val ->
              Server.cancel_transaction(db, tx_ref)
              exit(val)
          after
            MVCC.unpin(mvcc, tx_ref)
          end

        case result do
          {:commit, tx, reply} ->
            case Server.commit_transaction(db, tx_ref, tx) do
              :ok -> reply
              error -> raise "Unable to commit due to following error: #{inspect(error)}"
            end

          {:abort, reply} ->
            Server.cancel_transaction(db, tx_ref)
            reply

          _ ->
            Server.cancel_transaction(db, tx_ref)
            raise "Invalid return from `Goblin.transaction/2`"
        end

      {:error, :nested_transaction} ->
        raise "Cannot start a transaction from within a transaction"
    end
  end

  @doc """
  Writes a key-value pair to the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - Any Elixir term to use as the key
  - `value` - Any Elixir term to be associated with `key`
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the key under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put(db, :alice, "Alice")
      # => :ok

      Goblin.put(db, :alice, "Alice", tag: :admins)
      # => :ok
  """
  @spec put(:gen_statem.server_ref(), term(), keyword()) :: :ok
  def put(db, key, val, opts \\ []) do
    put_multi(db, [{key, val}], opts)
  end

  @doc """
  Writes multiple key-value pairs in a single transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `pairs` - A list of `{key, value}` tuples
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}])
      # => :ok
  """
  @spec put_multi(:gen_statem.server_ref(), Enumerable.t({term(), term()}), keyword()) ::
          :ok
  def put_multi(db, pairs, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Tx.put_multi(pairs, opts)
      |> Tx.commit()
    end)
  end

  @doc """
  Updates a key.

  Updates the value corresponding to `key` either via the provided function or via `default`. 
  See `update_multi` for more information.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to update
  - `updater` - A function that updates the value
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:default` - Default value if key does not already exist
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.update(db, :counter, &(&1 + 1), default: 1)
      # => :ok
  """
  @spec update(
          :gen_statem.server_ref(),
          term(),
          (term() -> term()) | (term(), term() -> term()),
          keyword()
        ) ::
          :ok
  def update(db, key, updater, opts \\ []) do
    {:ok, _} = update_multi(db, [key], updater, opts)
    :ok
  end

  @doc """
  Updates multiple keys.

  Updates multiple values corresponding to multiple keys via the provided function.
  The update function can be of either arity 1 or 2.
  With arity 1, the function receives only the previous value.
  With arity 2, the function receives both the key and the previous value. 
  For any non-existing keys, they are inserted with the `:default` option.
  If `:default` is not provided, then non-existing keys are not inserted.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - The keys to update
  - `updater` - A function that updates the value
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:default` - Default value for non-existing keys
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `{:ok, no_entries_updated}`

  ## Examples

      Goblin.update_multi(db, [:alice, :bob, :charlie], &String.upcase(&1))
      # => {:ok, 3}

      Goblin.update_multi(db, [:alice, :bob, :charlie], fn k, v ->
        if k == :bob,
          do: String.upcase(v),
          else: String.downcase(v)
      end)
      # => {:ok, 3}
  """
  @spec update_multi(
          :gen_statem.server_ref(),
          list(term()),
          (term() -> term()) | (term(), term() -> term()),
          keyword()
        ) ::
          {:ok, non_neg_integer()}
  def update_multi(db, keys, updater, opts \\ []) do
    keys =
      keys
      |> Enum.sort(:desc)
      |> Enum.reduce([], fn
        key1, [key2 | _] = acc when key1 == key2 -> acc
        key, acc -> [key | acc]
      end)

    transaction(db, fn tx ->
      found = Tx.get_multi(tx, keys, opts)

      updated =
        Enum.map(found, fn {key, old} ->
          if is_function(updater, 2),
            do: {key, updater.(key, old)},
            else: {key, updater.(old)}
        end)

      inserted =
        case Keyword.fetch(opts, :default) do
          {:ok, default} ->
            found_keys = MapSet.new(found, &elem(&1, 0))
            for k <- keys, k not in found_keys, do: {k, default}

          :error ->
            []
        end

      new = inserted ++ updated

      tx
      |> Tx.put_multi(new, opts)
      |> Tx.commit({:ok, length(new)})
    end)
  end

  @doc """
  Updates a key and returns a value.

  Updates a value corresponding to `key` via the provided function and can return an arbitrary value.
  The provided function gets the previous value (`nil` if previously not set) as the argument.
  The provided function must return a two-tuple with the first element as the return value and the second element as the updated value.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to update
  - `updater` - A function that updates the value
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - Reply from the provided function

  ## Examples

      Goblin.get_and_update(db, :alice, fn name -> 
        {String.upcase(name), name <> "alice"} 
      end)
      # => "ALICE"
  """
  @spec get_and_update(:gen_statem.server_ref(), term(), (term() -> {term(), term()}), keyword()) ::
          term()
  def get_and_update(db, key, updater, opts \\ []) do
    transaction(db, fn tx ->
      old = Tx.get(tx, key, opts)
      {reply, new} = updater.(old)

      tx
      |> Tx.put(key, new, opts)
      |> Tx.commit(reply)
    end)
  end

  @doc """
  Updates multiple keys and returns a value.

  Gets and updates multiple keys in a single transaction via a provided function.
  Any keys that are not already present in the database receive the value set in the `:default` option, if provided, otherwise excluded.
  The provided function must return a two-tuple with the first element as the return value and the second element as the updated value.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - The key to update
  - `updater` - A function that updates the value
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:default` - Default value for non-existing keys
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - Reply from the provided function

  ## Examples

      Goblin.get_and_update_multi(db, [:alice, :bob, :charlie], fn entries -> 
        entries = Enum.into(entries, %{}, fn {key, val} -> {key, String.upcase(val)} end)
        {Map.keys(entries), entries}
      end)
      # => [:alice, :bob, :charlie]
  """
  @spec get_and_update_multi(
          :gen_statem.server_ref(),
          list(term()),
          (map() -> {term(), map()}),
          keyword()
        ) :: term()
  def get_and_update_multi(db, keys, updater, opts \\ []) do
    transaction(db, fn tx ->
      old = Tx.get_multi(tx, keys, opts) |> Map.new()

      entries =
        case Keyword.fetch(opts, :default) do
          {:ok, default} -> Map.new(keys, &{&1, default}) |> Map.merge(old)
          :error -> old
        end

      {reply, new} = updater.(entries)

      tx
      |> Tx.put_multi(new, opts)
      |> Tx.commit(reply)
    end)
  end

  @doc """
  Compare and swap a key.

  Compare and swap a value corresponding to `key`. 
  Returns `true` if swapped, `false` otherwise.

  Comparison is done via `==`.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to update
  - `old` - The value to compare with
  - `new` - The value to swap to
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag to namespace the keys under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `true` if swapped
  - `false` if not swapped

  ## Examples

      Goblin.cas(db, :alice, "alice", "ALICE")
      # => true
  """
  @spec cas(:gen_statem.server_ref(), term(), term(), term(), keyword()) :: boolean()
  def cas(db, key, old, new, opts \\ []) do
    transaction(db, fn tx ->
      case Tx.get(tx, key, opts) do
        from_store when from_store == old ->
          tx
          |> Tx.put(key, new, opts)
          |> Tx.commit(true)

        _ ->
          tx
          |> Tx.abort(false)
      end
    end)
  end

  @doc """
  Removes a key from the database.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove(db, :alice)
      # => :ok

      Goblin.get(db, :alice)
      # => nil
  """
  def remove(db, key, opts \\ []) do
    remove_multi(db, [key], opts)
  end

  @doc """
  Removes multiple keys from the database in a single transaction.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to remove
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under
    - `:timeout` - Timeout (in milliseconds) for the calls (default: `:infinity`)

  ## Returns

  - `:ok`

  ## Examples

      Goblin.remove_multi(db, [:alice, :bob, :charlie])
      # => :ok
  """
  def remove_multi(db, keys, opts \\ []) do
    transaction(db, fn tx ->
      tx
      |> Tx.remove_multi(keys, opts)
      |> Tx.commit()
    end)
  end

  @doc """
  Performs a read-only transaction.

  A snapshot is taken to provide a consistent mvcc of the database.
  Multiple readers run concurrently without blocking each other.
  Attempting to write within a read transaction raises.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `callback` - A function that takes a `Goblin.Tx.t()` struct

  ## Returns

  - The return value of `callback`

  ## Examples

      Goblin.read(db, fn tx ->
        alice = Goblin.Tx.get(tx, :alice)
        bob = Goblin.Tx.get(tx, :bob)
        {alice, bob}
      end)
      # => {"Alice", "Bob"}
  """
  def read(db, callback) do
    {db, mvcc} = server_info(db)
    tx_ref = make_ref()

    try do
      Process.link(db)
      {sqn, max_lk} = MVCC.pin(mvcc, tx_ref)

      %Tx{
        ref: tx_ref,
        mvcc: mvcc,
        sqn: sqn,
        max_level_key: max_lk
      }
      |> callback.()
    after
      MVCC.unpin(mvcc, tx_ref)
      Process.unlink(db)
    end
  end

  @doc """
  Retrieves the value associated with a key.

  Returns the default value if the key is not found.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `key` - The key to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the key is namespaced under
    - `:default` - Value to return if `key` is not found (default: `nil`)

  ## Returns

  - The value associated with the key, or `default` if not found

  ## Examples

      Goblin.get(db, :alice)
      # => "Alice"

      Goblin.get(db, :nonexistent)
      # => nil

      Goblin.get(db, :nonexistent, default: :not_found)
      # => :not_found

      Goblin.get(db, :alice, tag: :admins)
      # => "Alice"
  """
  def get(db, key, opts \\ []) do
    read(db, fn tx -> Tx.get(tx, key, opts) end)
  end

  @doc """
  Retrieves values for multiple keys in a single read.

  Keys not found in the database are excluded from the result.

  Raises `Goblin.IOError` if the underlying storage cannot be read.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `keys` - A list of keys to look up
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A list of `{key, value}` tuples for keys found, in unspecified order

  ## Examples

      Goblin.get_multi(db, [:alice, :bob])
      # => [{:alice, "Alice"}, {:bob, "Bob"}]

      Goblin.get_multi(db, [:alice, :nonexistent])
      # => [{:alice, "Alice"}]
  """
  def get_multi(db, keys, opts \\ []) do
    read(db, fn tx -> Tx.get_multi(tx, keys, opts) end)
  end

  @doc """
  Checks whether a key is a member of the database or not.

  > #### False positives {: .note}
  >
  > If the key has been flushed to disk, then membership is checked via the disk table's Bloom filters, i.e. it can yield a false positive in some cases.

  ## Parameters
    
  - `db` - The database server (PID or registered name)
  - `key` - The key to check membership off
  - `opts` - A keyword list with the following options (default: `[]`):
    - `:tag` - Tag the keys are namespaced under

  ## Returns

  - A boolean indicating if the key exists or not

  ## Examples

      Goblin.has_key?(db, :alice)
      # => false
      Goblin.put(db, :alice, "Alice")
      Goblin.has_key?(db, :alice)
      # => true
  """
  def has_key?(db, key, opts \\ []) do
    read(db, fn tx -> Tx.has_key?(tx, key, opts) end)
  end

  @doc """
  Exports a snapshot of the database as a `.tar.gz` archive.

  The archive can be unpacked and used as the `data_dir` for a new
  database instance, acting as a backup.

  The export is run inside the server,
  thus blocking file deletion and writes until completed.

  ## Parameters

  - `db` - The database server (PID or registered name)
  - `export_dir` - Directory to place the exported `.tar.gz` file

  ## Returns

  - `{:ok, export_path}` - Path to the created archive
  - `{:error, reason}` - If an error occurred

  ## Examples

      Goblin.export(db, "/backups")
      # => {:ok, "/backups/goblin_20260220T120000Z.tar.gz"}
  """
  @spec export(Server.t(), Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def export(db, export_dir), do: Server.export(db, export_dir)

  @doc """
  Returns whether a memory-to-disk flush is currently running.
  """
  @spec flushing?(Server.t()) :: boolean()
  def flushing?(db, timeout \\ 5_000), do: Server.flushing?(db, timeout)

  @doc """
  Returns whether any background compaction is currently in progress.
  """
  @spec compacting?(Server.t(), keyword()) :: boolean()
  def compacting?(db, timeout \\ 5_000), do: Server.compacting?(db, timeout)
  # defdelegate compacting?(db, timeout \\ 5_000), to: Server

  @doc """
  Starts the database.

  Creates the `data_dir` if it does not exist.

  ## Options

  - `:name` - Registered name for the database (optional, defaults to `Goblin`)
  - `:data_dir` - Directory path for database files (required)
  - `:mem_limit` - Bytes to buffer in memory before flushing to disk (default: 64 MB)
  - `:bf_fpp` - Bloom filter false positive probability (default: 0.01)

  ## Returns

  - `{:ok, pid}` - On successful start
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, db} = Goblin.start_link(
        name: MyApp.DB,
        data_dir: "/var/lib/myapp/db"
      )
  """
  @spec start_link(keyword()) :: :gen_statem.start_ret()
  defdelegate start_link(opts), to: Server

  @doc """
  Starts the database, see `start_link/1` for more details.
  """
  @spec start(keyword()) :: :gen_statem.start_ret()
  defdelegate start(opts), to: Server

  @doc """
  Stops the database.
  """
  @spec stop(:gen_statem.server_ref(), term(), timeout()) :: :ok
  defdelegate stop(db, reason \\ :normal, timeout \\ :infinity), to: Server

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: Server

  defp server_info(db) do
    pid = pid_of(db)

    mvcc =
      (pid && :persistent_term.get({Goblin, pid}, nil)) ||
        raise ArgumentError, "Goblin database #{inspect(db)} is not running or still starting"

    {pid, mvcc}
  end

  defp pid_of(pid) when is_pid(pid), do: pid
  defp pid_of(name), do: Process.whereis(name)
end
