defmodule Goblin.Writer do
  @moduledoc false
  use GenServer
  import Goblin.ProcessRegistry, only: [via: 1]
  require Logger
  alias Goblin.Writer.MemTable
  alias Goblin.Writer.Transaction
  alias Goblin.Reader
  alias Goblin.Store
  alias Goblin.WAL
  alias Goblin.Manifest
  alias Goblin.SSTs
  alias Goblin.PubSub

  @flush_level 0

  defstruct [
    :registry,
    :pub_sub,
    :task_sup,
    :task_mod,
    :key_limit,
    seq: 0,
    mem_table: MemTable.new(),
    transactions: %{},
    flushing: []
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    registry = opts[:registry]

    args =
      Keyword.take(opts, [
        :registry,
        :pub_sub,
        :task_sup,
        :task_mod,
        :key_limit
      ])

    GenServer.start_link(__MODULE__, args, name: via(registry))
  end

  @spec get(Goblin.registry(), Goblin.db_key()) :: {:ok, Goblin.db_value()} | :error
  def get(registry, key) do
    GenServer.call(via(registry), {:get, key})
  end

  @spec get_multi(Goblin.registry(), [Goblin.db_key()]) :: {:ok, Goblin.db_value()} | :error
  def get_multi(registry, keys) do
    GenServer.call(via(registry), {:get_multi, keys})
  end

  @spec get_iterators(Goblin.registry()) :: [
          {(-> [Goblin.triple()]),
           ([Goblin.triple()] -> :ok | {Goblin.triple(), [Goblin.triple()]})}
        ]
  def get_iterators(registry) do
    GenServer.call(via(registry), :get_iterators)
  end

  @spec put(Goblin.registry(), Goblin.db_key(), Goblin.db_value()) :: :ok | {:error, term()}
  def put(registry, key, value) do
    transaction(registry, fn tx ->
      tx = Transaction.put(tx, key, value)
      {:commit, tx, :ok}
    end)
  end

  @spec put_multi(Goblin.registry(), [{Goblin.db_key(), Goblin.db_value()}]) ::
          :ok | {:error, term()}
  def put_multi(registry, pairs) do
    transaction(registry, fn tx ->
      tx = Enum.reduce(pairs, tx, fn {k, v}, acc -> Transaction.put(acc, k, v) end)
      {:commit, tx, :ok}
    end)
  end

  @spec remove(Goblin.registry(), Goblin.db_key()) :: :ok | {:error, term()}
  def remove(registry, key) do
    transaction(registry, fn tx ->
      tx = Transaction.remove(tx, key)
      {:commit, tx, :ok}
    end)
  end

  @spec remove_multi(Goblin.registry(), [Goblin.db_key()]) :: :ok | {:error, term()}
  def remove_multi(registry, keys) do
    transaction(registry, fn tx ->
      tx = Enum.reduce(keys, tx, fn k, acc -> Transaction.remove(acc, k) end)
      {:commit, tx, :ok}
    end)
  end

  @spec is_flushing(Goblin.registry()) :: boolean()
  def is_flushing(registry), do: GenServer.call(via(registry), :is_flushing)

  @spec transaction(Goblin.registry(), (Transaction.t() -> Goblin.transaction_return())) ::
          term() | :ok | {:error, term()}
  def transaction(registry, f) do
    writer = via(registry)

    with {:ok, tx} <- start_transaction(writer, self()),
         {:ok, tx, reply} <- run_transaction(writer, f, tx),
         :ok <- commit_transaction(writer, tx, self()) do
      reply
    end
  end

  defp start_transaction(writer, pid), do: GenServer.call(writer, {:start_transaction, pid})

  defp run_transaction(writer, f, tx) do
    case f.(tx) do
      {:commit, tx, reply} ->
        {:ok, tx, reply}

      :cancel ->
        cancel_transaction(writer, self())

      _ ->
        cancel_transaction(writer, self())
        raise "Invalid return type from transaction."
    end
  end

  defp commit_transaction(writer, tx, pid) do
    GenServer.call(writer, {:commit_transaction, tx, pid})
  end

  defp cancel_transaction(writer, pid) do
    GenServer.call(writer, {:cancel_transaction, pid})
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       registry: args[:registry],
       pub_sub: args[:pub_sub],
       task_sup: args[:task_sup],
       task_mod: args[:task_mod] || Task.Supervisor,
       key_limit: args[:key_limit],
       mem_table: MemTable.new()
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:get, key}, _from, state) do
    flushing_mem_tables =
      state.flushing
      |> Enum.map(fn {_, mem_table, _, _} -> mem_table end)

    mem_tables = [state.mem_table | flushing_mem_tables]
    reply = search_for_key(mem_tables, key)
    {:reply, reply, state}
  end

  def handle_call({:get_multi, keys}, _from, state) do
    flushing_mem_tables =
      state.flushing
      |> Enum.map(fn {_, mem_table, _, _} -> mem_table end)

    mem_tables = [state.mem_table | flushing_mem_tables]

    reply =
      Enum.reduce(keys, {[], []}, fn key, {found, not_found} ->
        case search_for_key(mem_tables, key) do
          :not_found -> {found, [key | not_found]}
          {:ok, {:value, seq, value}} -> {[{key, seq, value} | found], not_found}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call(:get_iterators, _from, state) do
    current_iterator = into_iterator(state.mem_table)

    flushing_iterators =
      state.flushing
      |> Enum.map(fn {_, mem_table, _, _} ->
        into_iterator(mem_table)
      end)

    {:reply, [current_iterator | flushing_iterators], state}
  end

  def handle_call({:start_transaction, pid}, _from, state) do
    if not Map.has_key?(state.transactions, pid) do
      registry = state.registry
      reader = &Reader.get(&1, registry)
      tx = Transaction.new(pid, reader)
      transactions = Map.put(state.transactions, pid, [])
      {:reply, {:ok, tx}, %{state | transactions: transactions}}
    else
      {:reply, {:error, :already_in_transaction}, state}
    end
  end

  def handle_call({:commit_transaction, tx, pid}, _from, state) do
    case Map.get(state.transactions, pid) do
      nil ->
        {:reply, {:error, :no_tx_found}, state}

      commits ->
        if not Transaction.has_conflict(tx, commits) do
          transactions =
            state.transactions
            |> clean_transaction(pid)
            |> add_commit_to_running_transactions(tx.mem_table)

          writes = Enum.map(tx.writes, &advance_seq_in_write(&1, state.seq))
          WAL.append(state.registry, writes)
          tx_mem_table = MemTable.advance_seq(tx.mem_table, state.seq)
          mem_table = MemTable.merge(state.mem_table, tx_mem_table)

          publish_writes(state, writes)

          state = %{
            state
            | transactions: transactions,
              mem_table: mem_table,
              seq: state.seq + length(writes)
          }

          {:reply, :ok, state, {:continue, :flush}}
        else
          state = %{state | transactions: clean_transaction(state.transactions, pid)}
          {:reply, {:error, :in_conflict}, state}
        end
    end
  end

  def handle_call({:cancel_transaction, pid}, _from, state) do
    transactions = Map.delete(state.transactions, pid)
    state = %{state | transactions: transactions}
    {:reply, :ok, state}
  end

  def handle_call(:is_flushing, _from, state) do
    is_flushing = length(state.flushing) != 0
    {:reply, is_flushing, state}
  end

  @impl GenServer
  def handle_continue(:recover_state, state) do
    case recover_writes(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, _reason} = error ->
        {:stop, error, state}
    end
  end

  def handle_continue(:flush, state) do
    case maybe_flush(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, _reason} = error ->
        {:stop, error, state}
    end
  end

  @impl GenServer
  def handle_info({ref, {:ok, :flushed}}, state) do
    state = clean_flush(state, ref)
    {:noreply, state}
  end

  def handle_info({ref, {:error, reason}}, state) do
    case Enum.find(state.flushing, fn
           {^ref, _, _, _} -> true
           _ -> false
         end) do
      {_, _, _, 0} ->
        Logger.error(fn ->
          "Flush failed after 5 attempts with reason: #{inspect(reason)}. Exiting."
        end)

        {:stop, {:error, :failed_to_flush}, state}

      {_, mem_table, rotated_wal, retry} ->
        Logger.warning(fn ->
          "Flush failed with reason: #{inspect(reason)}. Retrying..."
        end)

        state = clean_flush(state, ref)
        flush = flush(state, rotated_wal, mem_table, retry - 1)
        state = %{state | flushing: [flush | state.flushing]}
        {:noreply, state}

      _ ->
        state = clean_flush(state, ref)
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp clean_flush(state, ref) do
    flushing =
      Enum.reject(state.flushing, fn
        {^ref, _, _, _} -> true
        _ -> false
      end)

    %{state | flushing: flushing}
  end

  defp clean_transaction(transactions, pid), do: Map.delete(transactions, pid)

  defp add_commit_to_running_transactions(transactions, mem_table),
    do: Enum.into(transactions, %{}, fn {pid, commits} -> {pid, [mem_table | commits]} end)

  defp advance_seq_in_write({seq1, :put, k, v}, seq2), do: {seq1 + seq2, :put, k, v}
  defp advance_seq_in_write({seq1, :remove, k}, seq2), do: {seq1 + seq2, :remove, k}

  defp maybe_flush(state, rotated_wal \\ nil) do
    if MemTable.has_overflow(state.mem_table, state.key_limit) do
      start_flush(state, rotated_wal)
    else
      {:ok, state}
    end
  end

  defp start_flush(state, rotated_wal) do
    with {:ok, rotated_wal} <- maybe_rotate(state, rotated_wal) do
      flush = flush(state, rotated_wal)
      state = %{state | mem_table: MemTable.new(), flushing: [flush | state.flushing]}
      {:ok, state}
    end
  end

  defp flush(state, rotated_wal, mem_table \\ nil, retry \\ 5) do
    mem_table = mem_table || state.mem_table

    %{
      registry: registry,
      task_sup: task_sup,
      task_mod: task_mod,
      key_limit: key_limit
    } = state

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        data = mem_table |> MemTable.sort() |> MemTable.flatten()

        with {:ok, flushed} <-
               SSTs.flush(data, @flush_level, key_limit, fn -> Store.new_file(registry) end),
             :ok <- Manifest.log_flush(registry, Enum.map(flushed, & &1.file), rotated_wal),
             :ok <- WAL.clean(registry, rotated_wal),
             :ok <- put_in_store(flushed, registry) do
          {:ok, :flushed}
        end
      end)

    {ref, mem_table, rotated_wal, retry}
  end

  defp publish_writes(state, writes) do
    %{
      pub_sub: pub_sub,
      task_mod: task_mod,
      task_sup: task_sup
    } = state

    task_mod.async(task_sup, fn ->
      writes =
        Enum.map(writes, fn
          {_seq, :put, k, v} -> {:put, k, v}
          {_seq, :remove, k} -> {:remove, k}
        end)

      PubSub.publish(pub_sub, writes)
    end)
  end

  defp put_in_store([], _store), do: :ok

  defp put_in_store([sst | ssts], registry) do
    Store.put(registry, sst)
    put_in_store(ssts, registry)
  end

  defp maybe_rotate(state, nil) do
    %{
      registry: registry,
      seq: seq
    } = state

    with {:ok, rotated_wal} <- WAL.rotate(registry),
         :ok <- Manifest.log_rotation(registry, rotated_wal),
         :ok <- Manifest.log_sequence(registry, seq) do
      {:ok, rotated_wal}
    end
  end

  defp maybe_rotate(_state, rotated_wal), do: {:ok, rotated_wal}

  defp search_for_key([], _key), do: :not_found

  defp search_for_key([mem_table | mem_tables], key) do
    case MemTable.read(mem_table, key) do
      {:value, seq, value} ->
        {:ok, {:value, seq, value}}

      :not_found ->
        search_for_key(mem_tables, key)
    end
  end

  defp into_iterator(mem_table) do
    {fn -> mem_table |> MemTable.sort() |> MemTable.flatten() end,
     fn
       [] -> :ok
       [next | data] -> {next, data}
     end}
  end

  defp recover_writes(state) do
    %{seq: seq} = Manifest.get_version(state.registry, [:seq])

    with {:ok, logs} <- WAL.recover(state.registry) do
      recover_state(logs, %{state | seq: seq})
    end
  end

  defp recover_state([], state), do: {:ok, state}

  defp recover_state([{rotated_wal_file, writes} | logs], state) do
    state = Enum.reduce(writes, state, &apply_write/2)

    with {:ok, state} <- maybe_flush(state, rotated_wal_file) do
      recover_state(logs, state)
    end
  end

  defp apply_write({seq, :put, k, v}, state) do
    mem_table = MemTable.upsert(state.mem_table, seq, k, v)
    %{state | mem_table: mem_table, seq: seq}
  end

  defp apply_write({seq, :remove, k}, state) do
    mem_table = MemTable.delete(state.mem_table, seq, k)
    %{state | mem_table: mem_table, seq: seq}
  end
end
