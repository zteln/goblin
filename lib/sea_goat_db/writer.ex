defmodule SeaGoatDB.Writer do
  @moduledoc false
  use GenServer
  alias SeaGoatDB.Writer.MemTable
  alias SeaGoatDB.Writer.Transaction
  # alias SeaGoatDB.Writer.FlushQueue
  alias SeaGoatDB.Reader
  alias SeaGoatDB.Store

  @type writer :: GenServer.server()
  @type transaction_return :: {:commit, Transaction.t(), term()} | :cancel

  @flush_level 0

  defstruct [
    :wal,
    :store,
    :manifest,
    :key_limit,
    :sst_mod,
    :wal_mod,
    :manifest_mod,
    seq: 0,
    mem_table: MemTable.new(),
    transactions: %{},
    flushing: [],
    subscribers: %{}
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [
        :wal,
        :store,
        :manifest,
        :key_limit,
        :sst_mod,
        :wal_mod,
        :manifest_mod
      ])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  # def subscribe(server, pid \\ self()) do
  #   GenServer.call(server, {:subscribe, pid})
  # end

  @spec get(writer(), key :: SeaGoatDB.db_key()) :: {:ok, SeaGoatDB.db_value()} | :error
  def get(writer, key) do
    GenServer.call(writer, {:get, key})
  end

  @spec put(writer(), SeaGoatDB.db_key(), SeaGoatDB.db_value()) :: :ok
  def put(writer, key, value) do
    transaction(writer, fn tx ->
      tx = Transaction.put(tx, key, value)
      {:commit, tx, :ok}
    end)
  end

  @spec remove(writer(), SeaGoatDB.db_key()) :: :ok
  def remove(writer, key) do
    transaction(writer, fn tx ->
      tx = Transaction.remove(tx, key)
      {:commit, tx, :ok}
    end)
  end

  def is_flushing(writer), do: GenServer.call(writer, :is_flushing)

  @spec transaction(writer(), (Transaction.t() -> transaction_return())) ::
          term() | :ok | {:error, term()}
  def transaction(writer, f) do
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
    wal = args[:wal]
    store = args[:store]
    manifest = args[:manifest]

    {:ok,
     %__MODULE__{
       wal: wal,
       store: store,
       manifest: manifest,
       key_limit: args[:key_limit],
       sst_mod: args[:sst_mod] || SeaGoatDB.SSTs,
       wal_mod: args[:wal_mod] || SeaGoatDB.WAL,
       manifest_mod: args[:manifest_mod] || SeaGoatDB.Manifest,
       mem_table: MemTable.new()
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:get, key}, _from, state) do
    flushing_mem_tables =
      state.flushing
      |> Enum.map(fn {_ref, mem_table} -> mem_table end)

    mem_tables = [state.mem_table | flushing_mem_tables]
    reply = search_for_key(mem_tables, key)
    {:reply, reply, state}
  end

  def handle_call({:start_transaction, pid}, _from, state) do
    if not Map.has_key?(state.transactions, pid) do
      writer = self()
      store = state.store
      reader = &Reader.get(&1, writer, store)
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
          state.wal_mod.append(state.wal, writes)
          tx_mem_table = MemTable.advance_seq(tx.mem_table, state.seq)
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
    flushing =
      Enum.reject(state.flushing, fn
        {^ref, _} -> true
        _ -> false
      end)

    {:noreply, %{state | flushing: flushing}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
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
      flushing = flush(state, rotated_wal)
      state = %{state | mem_table: MemTable.new(), flushing: [flushing | state.flushing]}
      {:ok, state}
    end
  end

  defp flush(state, rotated_wal, mem_table \\ nil) do
    mem_table = mem_table || state.mem_table

    %{
      store: store,
      sst_mod: sst_mod,
      wal: wal,
      wal_mod: wal_mod,
      manifest: manifest,
      manifest_mod: manifest_mod,
      key_limit: key_limit
    } = state

    %{ref: ref} =
      Task.async(fn ->
        data = mem_table |> MemTable.sort() |> MemTable.flatten()

        with {:ok, flushed} <-
               sst_mod.flush(data, @flush_level, key_limit, fn -> Store.new_file(store) end),
             :ok <- manifest_mod.log_flush(manifest, Enum.map(flushed, &elem(&1, 0)), rotated_wal),
             :ok <- wal_mod.clean(wal, rotated_wal),
             :ok <- put_in_store(flushed, store) do
          {:ok, :flushed}
        end
      end)

    {ref, mem_table}
  end

  defp put_in_store([], _store), do: :ok

  defp put_in_store([{file, {bloom_filter, priority, size, key_range}} | flushed], store) do
    Store.put(store, file, @flush_level, bloom_filter, priority, size, key_range)
    put_in_store(flushed, store)
  end

  defp maybe_rotate(state, nil) do
    %{
      wal: wal,
      wal_mod: wal_mod,
      manifest: manifest,
      manifest_mod: manifest_mod,
      seq: seq
    } = state

    with {:ok, rotated_wal} <- wal_mod.rotate(wal),
         :ok <- manifest_mod.log_rotation(manifest, rotated_wal),
         :ok <- manifest_mod.log_sequence(manifest, seq) do
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

  defp recover_writes(state) do
    %{seq: seq} = state.manifest_mod.get_version(state.manifest, [:seq])

    with {:ok, logs} <- state.wal_mod.recover(state.wal) do
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
