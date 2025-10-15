defmodule SeaGoat.Writer do
  use GenServer
  alias SeaGoat.Writer.MemTable
  alias SeaGoat.Writer.Transaction
  alias SeaGoat.Writer.Flusher
  alias SeaGoat.WAL
  alias SeaGoat.Manifest

  @type writer :: GenServer.server()
  @type transaction_return :: {:commit, Transaction.t(), term()} | :cancel

  defstruct [
    :wal,
    :store,
    :manifest,
    :key_limit,
    :flushing,
    :flusher,
    seq: 0,
    mem_table: MemTable.new(),
    transactions: %{},
    flush_queue: :queue.new(),
    subscribers: %{}
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [
        :wal,
        :store,
        :manifest,
        :flusher,
        :key_limit
      ])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  # def subscribe(server, pid \\ self()) do
  #   GenServer.call(server, {:subscribe, pid})
  # end

  @doc "Gets a value from in-memory MemTables."
  @spec get(writer(), key :: SeaGoat.db_key()) :: {:ok, SeaGoat.db_value()} | :error
  def get(writer, key) do
    GenServer.call(writer, {:get, key})
  end

  @doc "Puts a key-value pair in the writers current MemTable."
  @spec put(writer(), SeaGoat.db_key(), SeaGoat.db_value()) :: :ok
  def put(writer, key, value) do
    transaction(writer, fn tx ->
      tx = Transaction.put(tx, key, value)
      {:commit, tx, :ok}
    end)
  end

  @doc "Removes the value corresponding to `key` in the writers current MemTable."
  @spec remove(writer(), SeaGoat.db_key()) :: :ok
  def remove(writer, key) do
    transaction(writer, fn tx ->
      tx = Transaction.remove(tx, key)
      {:commit, tx, :ok}
    end)
  end

  @doc "Runs a transaction."
  @spec transaction(writer(), (Transaction.t() -> transaction_return())) :: term()
  def transaction(writer, f) do
    with {:ok, tx} <- start_transaction(writer, self()) do
      run_transaction(writer, f, tx)
    end
  end

  defp start_transaction(writer, pid), do: GenServer.call(writer, {:start_transaction, pid})

  defp run_transaction(writer, f, tx) do
    case f.(tx) do
      {:commit, tx, reply} ->
        :ok = commit_transaction(writer, tx, self())
        reply

      :cancel ->
        cancel_transaction(writer, self())

      _ ->
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
    wal = args[:wal]
    store = args[:store]
    manifest = args[:manifest]

    {:ok,
     %__MODULE__{
       wal: wal,
       store: store,
       manifest: manifest,
       key_limit: args[:key_limit],
       flusher: args[:flusher] || (&Flusher.flush(&1, &2, store, wal, manifest)),
       mem_table: MemTable.new()
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:get, key}, _from, state) do
    flushing_mem_table = if state.flushing, do: [elem(state.flushing, 1)], else: []

    flush_queue_mem_tables =
      state.flush_queue
      |> :queue.to_list()
      |> Enum.map(&elem(&1, 0))

    mem_tables = [state.mem_table | flush_queue_mem_tables ++ flushing_mem_table]

    case search_for_key(mem_tables, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, state}

      :not_found ->
        {:reply, :error, state}
    end
  end

  def handle_call({:start_transaction, pid}, _from, state) do
    if not Map.has_key?(state.transactions, pid) do
      writer = self()
      store = state.store
      tx = Transaction.new(pid, &SeaGoat.Reader.get(writer, store, &1))
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
          WAL.append(state.wal, writes)
          tx_mem_table = Enum.into(tx.mem_table, %{}, &advance_seq_in_mem_table(&1, state.seq))
          mem_table = MemTable.merge(state.mem_table, tx_mem_table)

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
    {:noreply, maybe_flush(state)}
  end

  @impl GenServer
  def handle_info({_ref, :flushed}, state) do
    {:noreply, %{state | flushing: nil}, {:continue, :flush}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp clean_transaction(transactions, pid), do: Map.delete(transactions, pid)

  defp add_commit_to_running_transactions(transactions, mem_table),
    do: Enum.into(transactions, %{}, fn {pid, commits} -> {pid, [mem_table | commits]} end)

  defp advance_seq_in_write({seq1, :put, k, v}, seq2), do: {seq1 + seq2, :put, k, v}
  defp advance_seq_in_write({seq1, :remove, k}, seq2), do: {seq1 + seq2, :remove, k}
  defp advance_seq_in_mem_table({key, {seq1, value}}, seq2), do: {key, {seq1 + seq2, value}}

  defp maybe_flush(state, rotated_wal \\ nil) do
    cond do
      queue_overflow?(state) -> queue_overflow(state, rotated_wal)
      flush_from_queue?(state) -> flush_from_queue(state)
      start_flush?(state) -> start_flush(state, rotated_wal)
      true -> state
    end
  end

  defp queue_overflow?(state) do
    MemTable.has_overflow(state.mem_table, state.key_limit) and not is_nil(state.flushing)
  end

  defp queue_overflow(state, rotated_wal) do
    rotated_wal = maybe_rotate(rotated_wal, state.wal, state.manifest, state.seq)

    flush_queue =
      :queue.in({state.mem_table, rotated_wal}, state.flush_queue)

    %{state | mem_table: MemTable.new(), flush_queue: flush_queue}
  end

  defp start_flush?(state) do
    MemTable.has_overflow(state.mem_table, state.key_limit) and :queue.len(state.flush_queue) == 0
  end

  defp start_flush(state, rotated_wal) do
    rotated_wal = maybe_rotate(rotated_wal, state.wal, state.manifest, state.seq)
    ref = state.flusher.(state.mem_table, rotated_wal)
    flushing = {ref, state.mem_table}
    %{state | mem_table: MemTable.new(), flushing: flushing}
  end

  defp flush_from_queue?(state) do
    is_nil(state.flushing) and :queue.len(state.flush_queue) > 0
  end

  defp flush_from_queue(state) do
    {{:value, {mem_table, rotated_wal}}, flush_queue} = :queue.out(state.flush_queue)
    ref = state.flusher.(mem_table, rotated_wal)
    flushing = {ref, mem_table}
    %{state | flushing: flushing, flush_queue: flush_queue}
  end

  defp maybe_rotate(nil, wal, manifest, seq) do
    {:ok, rotated_wal} = WAL.rotate(wal)
    Manifest.log_sequence(manifest, seq)
    rotated_wal
  end

  defp maybe_rotate(rotated_wal, _wal, _manifest, _seq), do: rotated_wal

  defp search_for_key([], _key), do: :not_found

  defp search_for_key([mem_table | mem_tables], key) do
    case MemTable.read(mem_table, key) do
      {:value, seq, value} ->
        {:ok, {:value, seq, value}}

      :not_found ->
        search_for_key(mem_tables, key)
    end
  end

  defp recover_writes(state) do
    with {:ok, writes} <- WAL.recover(state.wal) do
      %{seq: seq} = Manifest.get_version(state.manifest, [:seq])
      state = Enum.reduce(writes, %{state | seq: seq}, &recover_state/2)
      {:ok, state}
    end
  end

  defp recover_state({rotated_wal_file, writes}, state) do
    Enum.reduce(writes, state, &apply_write/2)
    |> maybe_flush(rotated_wal_file)
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
