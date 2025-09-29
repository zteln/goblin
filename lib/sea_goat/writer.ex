defmodule SeaGoat.Writer do
  use GenServer
  alias SeaGoat.Writer.MemTable
  alias SeaGoat.Writer.Transaction
  alias SeaGoat.WAL
  alias SeaGoat.SSTables
  alias SeaGoat.Store

  @flush_level 0
  @default_mem_limit 20000

  defstruct [
    :wal,
    :store,
    :limit,
    mem_table: MemTable.new(),
    transactions: %{},
    flushing: [],
    subscribers: %{}
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [:wal, :store, :limit])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  # def subscribe(server, pid \\ self()) do
  #   GenServer.call(server, {:subscribe, pid})
  # end

  def read(writer, key) do
    GenServer.call(writer, {:read, key})
  end

  @spec put(writer :: GenServer.server(), key :: term(), value :: term()) :: :ok
  def put(writer, key, value) do
    transaction(writer, fn tx ->
      tx = Transaction.put(tx, key, value)
      {:commit, tx, :ok}
    end)
  end

  @spec remove(writer :: GenServer.server(), key :: term()) :: :ok
  def remove(writer, key) do
    transaction(writer, fn tx ->
      tx = Transaction.remove(tx, key)
      {:commit, tx, :ok}
    end)
  end

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

      _ ->
        cancel_transaction(writer, self())
    end
  end

  defp commit_transaction(writer, tx, pid) do
    GenServer.call(writer, {:commit_transaction, tx, pid})
  end

  defp cancel_transaction(writer, pid) do
    GenServer.call(writer, {:cancel_transaction, pid})
  end

  @impl GenServer
  def init(opts) do
    {:ok,
     %__MODULE__{
       wal: opts[:wal],
       store: opts[:store],
       mem_table: MemTable.new(),
       limit: opts[:limit] || @default_mem_limit
     }, {:continue, :wait_for_replay}}
  end

  @impl GenServer
  def handle_call({:read, key}, _from, state) do
    mem_tables = [state.mem_table | Enum.map(state.flushing, &elem(&1, 1))]

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
            |> Map.delete(pid)
            |> Enum.into(%{}, fn {pid, commits} ->
              {pid, [tx.mem_table | commits]}
            end)

          WAL.append_batch(state.wal, tx.writes)
          mem_table = MemTable.merge(state.mem_table, tx.mem_table)
          state = %{state | transactions: transactions, mem_table: mem_table}
          {:reply, :ok, state, {:continue, :flush}}
        else
          transactions = Map.delete(state.transactions, pid)
          state = %{state | transactions: transactions}
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
  def handle_continue(:wait_for_replay, state) do
    state =
      receive do
        {:replay, commands} ->
          replay_commands(state, commands)
      end

    {:noreply, state}
  end

  def handle_continue(:flush, state) do
    {:noreply, maybe_flush(state)}
  end

  @impl GenServer
  def handle_info({ref, :flushed}, state) do
    flushing = Enum.reject(state.flushing, &(elem(&1, 0) == ref))
    state = %{state | flushing: flushing}
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp maybe_flush(state, path \\ nil) do
    if MemTable.has_overflow(state.mem_table, state.limit) do
      {path, tmp_path} = rotate_log(state, path)
      ref = flush(state.mem_table, path, tmp_path, state.store)
      flushing = [{ref, state.mem_table} | state.flushing]
      %{state | mem_table: MemTable.new(), flushing: flushing}
    else
      state
    end
  end

  defp rotate_log(state, nil) do
    path = WAL.current_file(state.wal)
    tmp_path = Store.tmp_path(path)
    new_path = Store.new_path(state.store)
    WAL.append(state.wal, {:del, [tmp_path]})
    WAL.rotate(state.wal, new_path)
    {path, tmp_path}
  end

  defp rotate_log(_state, path) do
    {path, Store.tmp_path(path)}
  end

  defp flush(mem_table, path, tmp_path, store) do
    %{ref: ref} =
      Task.async(fn ->
        with {:ok, bloom_filter, tmp_path, level} <-
               SSTables.write(
                 %SSTables.FlushIterator{},
                 mem_table,
                 tmp_path,
                 @flush_level
               ),
             :ok <- SSTables.switch(tmp_path, path),
             :ok <- Store.put(store, path, bloom_filter, level) do
          :flushed
        end
      end)

    ref
  end

  defp search_for_key([], _key), do: :not_found

  defp search_for_key([mem_table | mem_tables], key) do
    case MemTable.read(mem_table, key) do
      {:value, value} ->
        {:ok, value}

      :not_found ->
        search_for_key(mem_tables, key)
    end
  end

  def replay_commands(state, []), do: state

  def replay_commands(state, [{path, batch} | commands]) do
    batch
    |> Enum.reduce(state, &replay_command/2)
    |> maybe_flush(path)
    |> replay_commands(commands)
  end

  def replay_command({:del, paths}, state) do
    SSTables.delete(paths)
    state
  end

  def replay_command({:put, key, value}, state) do
    mem_table = MemTable.upsert(state.mem_table, key, value)
    %{state | mem_table: mem_table}
  end

  def replay_command({:remove, key}, state) do
    mem_table = MemTable.delete(state.mem_table, key)
    %{state | mem_table: mem_table}
  end
end
