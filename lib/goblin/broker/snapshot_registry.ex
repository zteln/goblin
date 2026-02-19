defmodule Goblin.Broker.SnapshotRegistry do
  @moduledoc false

  @type t :: :ets.table()
  @type table_id :: term()

  @max_retries 60

  @doc "Create a new snapshot registry."
  @spec new(atom()) :: t()
  def new(name) do
    :ets.new(name, [
      :named_table,
      :public,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  @doc "Generate a new snapshot reference, used for locking tables and snapshot lookup."
  @spec new_ref(t(), non_neg_integer()) :: reference()
  def new_ref(snapshot_registry, timeout \\ 30_000)

  def new_ref(_snapshot_registry, timeout) when timeout <= 0,
    do: raise("Snapshot registry failed to get ready within timeout.")

  def new_ref(snapshot_registry, timeout) do
    case :ets.lookup(snapshot_registry, :ready_flags) do
      [{:ready_flags, 2}] ->
        make_ref()

      _ ->
        Process.sleep(50)
        new_ref(snapshot_registry, timeout - 50)
    end
  end

  @doc "Increment ready flag."
  @spec inc_ready(t()) :: :ok
  def inc_ready(snapshot_registry) do
    :ets.update_counter(snapshot_registry, :ready_flags, {2, 1}, {:ready_flags, 0})
    :ok
  end

  @doc "Deincrement ready flag."
  @spec deinc_ready(t()) :: :ok
  def deinc_ready(snapshot_registry) do
    :ets.update_counter(snapshot_registry, :ready_flags, {2, -1, 0, 0}, {:ready_flags, 0})
    :ok
  end

  @doc "Add a table to the snapshot registry."
  @spec add_table(t(), table_id(), Goblin.level_key(), Goblin.Queryable.t(), (table_id() -> :ok)) ::
          :ok
  def add_table(snapshot_registry, id, level_key, table, delete_callback) do
    entry = %{table: table, delete_callback: delete_callback}
    :ets.insert(snapshot_registry, {{:table, id}, true, 0, level_key, entry})
    :ok
  end

  @doc "Soft deletes a table. No new snapshots after this can get this table any longer."
  @spec soft_delete(t(), table_id()) :: :ok
  def soft_delete(snapshot_registry, id) do
    case :ets.match_object(snapshot_registry, {{:table, id}, :_, :_, :_, :_}) do
      [] ->
        :ok

      [{key, _active?, _retries, _level_key, _entry}] ->
        :ets.update_element(snapshot_registry, key, {2, false})
        :ok
    end
  end

  @doc "Tries to delete inactive tables. Returns `{:error, :too_many_retries}` if a table has been attempted to be deleted too many times, otherwise `:ok`."
  @spec hard_delete(t()) :: :ok | {:error, :too_many_retries}
  def hard_delete(snapshot_registry) do
    :ets.match_object(snapshot_registry, {{:table, :_}, false, :_, :_, :_})
    |> Enum.reduce_while(:ok, fn
      {_key, _active?, retries, _level_key, _entry}, _acc when retries > @max_retries ->
        {:halt, {:error, :too_many_retries}}

      {{:table, id} = key, _active?, _retries, _level_key,
       %{table: table, delete_callback: delete_callback}},
      acc ->
        case count_snapshots(snapshot_registry, id) do
          0 ->
            delete_callback.(table)
            :ets.delete(snapshot_registry, key)

          _ ->
            :ets.update_counter(snapshot_registry, key, {3, 1})
        end

        {:cont, acc}
    end)
  end

  @doc "Filters the tables under a snapshot according to the given predicate."
  @spec filter_tables(t(), reference(), keyword()) ::
          list(Goblin.Queryable.t())
  def filter_tables(snapshot_registry, ref, opts \\ []) do
    predicate = opts[:filter] || fn _ -> true end
    level_key = opts[:level_key] || :_

    :ets.match_object(snapshot_registry, {{:snapshot, :_, ref}})
    |> Enum.flat_map(fn {{:snapshot, id, _ref}} ->
      case :ets.match_object(snapshot_registry, {{:table, id}, :_, :_, level_key, :_}) do
        [{_key, _active?, _retries, _level_key, %{table: table}}] ->
          case predicate.(table) do
            true -> [table]
            false -> []
          end

        _ ->
          []
      end
    end)
  end

  @doc "Registers a new snapshot with the provided reference. Returns the maximum level key for tables registered under the snapshot."
  @spec register_snapshot(t(), reference()) :: Goblin.level_key()
  def register_snapshot(snapshot_registry, ref) do
    :ets.match_object(snapshot_registry, {{:table, :_}, true, :_, :_, :_})
    |> Enum.reduce(
      -1,
      fn {{:table, id} = key, _active?, _retries, level_key, _entry}, acc ->
        snapshot_key = {:snapshot, id, ref}
        :ets.insert(snapshot_registry, {snapshot_key})
        :ets.member(snapshot_registry, key) || :ets.delete(snapshot_registry, snapshot_key)

        if level_key > acc, do: level_key, else: acc
      end
    )
  end

  @doc "Deletes the snapshot entries corresponding with the provided reference."
  @spec unregister_snapshot(t(), reference()) :: :ok
  def unregister_snapshot(snapshot_registry, ref) do
    :ets.match_delete(snapshot_registry, {{:snapshot, :_, ref}})
    :ok
  end

  defp count_snapshots(snapshot_registry, id) do
    :ets.match_object(snapshot_registry, {{:snapshot, id, :_}})
    |> Enum.count()
  end
end
