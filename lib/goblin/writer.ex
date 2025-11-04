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

  @spec transaction(
          Goblin.registry(),
          (Transaction.t() -> Goblin.transaction_return()),
          keyword()
        ) ::
          term() | :ok | {:error, term()}
  def transaction(registry, f, opts \\ []) do
    writer = via(registry)
    opts = Keyword.take(opts, [:timeout, :retries])
    do_transaction(writer, f, opts)
  end

  defp do_transaction(writer, f, opts) do
    with {:ok, tx} <- start_transaction(writer, opts),
         {:ok, tx, reply} <- run_transaction(writer, f, tx),
         :ok <- commit_transaction(writer, f, tx, opts) do
      reply
    end
  end

  defp start_transaction(writer, opts),
    do: GenServer.call(writer, {:start_transaction, self(), opts})

  defp run_transaction(writer, f, tx) do
    me = self()

    case f.(tx) do
      {:commit, tx, reply} ->
        {:ok, tx, reply}

      :cancel ->
        cancel_transaction(writer, me)

      _ ->
        cancel_transaction(writer, me)
        raise "Invalid return type from transaction."
    end
  end

  defp commit_transaction(writer, f, tx, opts) do
    me = self()

    case GenServer.call(writer, {:commit_transaction, tx, me}) do
      :ok ->
        :ok

      :retry ->
        opts = Keyword.replace(opts, :retries, (opts[:retries] || 0) - 1)
        do_transaction(writer, f, opts)

      {:error, _reason} = error ->
        error
    end
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
  def handle_call({:get, key}, {pid, _}, state) do
    reply =
      case Map.get(state.transactions, pid) do
        %{immutable_mem_tables: immutable_mem_tables} ->
          search_for_key(immutable_mem_tables, key)

        _ ->
          mem_tables = mem_tables_snapshot(state)
          search_for_key(mem_tables, key)
      end

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
          {:ok, {:value, seq, value}} -> {[{seq, key, value} | found], not_found}
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

  def handle_call({:start_transaction, pid, opts}, _from, state) do
    if not Map.has_key?(state.transactions, pid) do
      registry = state.registry
      immutable_mem_tables = mem_tables_snapshot(state)
      max_seq = state.seq
      reader = &Reader.get(&1, registry, max_seq)
      opts = Keyword.put(opts, :timestamp, now())
      tx = Transaction.new(pid, opts, reader)
      transactions =
        Map.put(state.transactions, pid, %{
          commits: [],
          immutable_mem_tables: immutable_mem_tables
        })

      {:reply, {:ok, tx}, %{state | transactions: transactions}}
    else
      {:reply, {:error, :already_in_transaction}, state}
    end
  end

  def handle_call({:commit_transaction, tx, pid}, _from, state) do
    case Map.get(state.transactions, pid) do
      nil ->
        {:reply, {:error, :tx_not_found}, state}

      %{commits: commits} ->
        cond do
          is_integer(tx.timeout) and now() - tx.timestamp >= tx.timeout ->
            state = %{state | transactions: clean_transaction(state.transactions, pid)}
            {:reply, {:error, :tx_timed_out}, state}

          Transaction.has_conflict(tx, commits) and tx.retries <= 0 ->
            state = %{state | transactions: clean_transaction(state.transactions, pid)}
            {:reply, {:error, :tx_in_conflict}, state}

          Transaction.has_conflict(tx, commits) ->
            state = %{state | transactions: clean_transaction(state.transactions, pid)}
            {:reply, :retry, state}

          true ->
            transactions =
              state.transactions
              |> clean_transaction(pid)
              |> add_commit_to_running_transactions(tx.mem_table)

            mem_table = merge_tx_mem_table(state.mem_table, tx.mem_table, state.seq)
            writes = Enum.map(tx.writes, &advance_seq_in_write(&1, state.seq))
            WAL.append(state.registry, writes)
            publish_writes(state, writes)

            state = %{
              state
              | transactions: transactions,
                mem_table: mem_table,
                seq: state.seq + length(writes)
            }

            {:reply, :ok, state, {:continue, :flush}}
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

  defp merge_tx_mem_table(mem_table, tx_mem_table, seq) do
    tx_mem_table = MemTable.advance_seq(tx_mem_table, seq)
    MemTable.merge(mem_table, tx_mem_table)
  end

  defp clean_flush(state, ref) do
    flushing =
      Enum.reject(state.flushing, fn
        {^ref, _, _, _} -> true
        _ -> false
      end)

    %{state | flushing: flushing}
  end

  defp clean_transaction(transactions, pid), do: Map.delete(transactions, pid)

  defp add_commit_to_running_transactions(transactions, mem_table) do
    Enum.into(transactions, %{}, fn {pid, tx_data} ->
      commits = [mem_table | tx_data.commits]
      {pid, Map.replace(tx_data, :commits, commits)}
    end)
  end

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
        stream =
          mem_table
          |> MemTable.sort()
          |> MemTable.flatten()
          |> flush_stream(key_limit)

        with {:ok, flushed} <-
               SSTs.new([stream], @flush_level, fn -> Store.new_file(registry) end),
             :ok <- Manifest.log_flush(registry, Enum.map(flushed, & &1.file), rotated_wal),
             :ok <- WAL.clean(registry, rotated_wal),
             :ok <- put_in_store(flushed, registry) do
          {:ok, :flushed}
        end
      end)

    {ref, mem_table, rotated_wal, retry}
  end

  defp flush_stream(data, key_limit) do
    Stream.resource(
      fn -> data end,
      &iter_flush_data/1,
      fn _ -> :ok end
    )
    |> Stream.chunk_every(key_limit)
  end

  defp iter_flush_data([]), do: {:halt, :ok}
  defp iter_flush_data([next | data]), do: {[next], data}

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

  defp mem_tables_snapshot(state) do
    flushing_mem_tables =
      state.flushing
      |> Enum.map(fn {_, mem_table, _, _} -> mem_table end)

    [state.mem_table | flushing_mem_tables]
  end

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

  defp now, do: System.monotonic_time(:millisecond)
end
