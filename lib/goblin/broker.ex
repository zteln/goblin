defmodule Goblin.Broker do
  @moduledoc false
  use GenServer

  alias Goblin.Broker.{
    ReadTx,
    WriteTx,
    SnapshotRegistry
  }

  alias Goblin.{
    MemTables,
    PubSub
  }

  import Goblin.Registry, only: [via: 2]

  defstruct [
    :writer,
    :mem_tables,
    :snapshot_registry,
    :pub_sub,
    :registry,
    write_queue: :queue.new()
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name] || __MODULE__

    args = [
      name: name,
      clean_up_interval: opts[:clean_up_interval],
      mem_tables: opts[:mem_tables],
      pub_sub: opts[:pub_sub],
      registry: opts[:registry]
    ]

    GenServer.start_link(__MODULE__, args, name: via(opts[:registry], name))
  end

  @spec write_transaction(:ets.table(), GenServer.server(), (Goblin.Tx.t() -> Goblin.Tx.return())) ::
          any() | {:error, :aborted}
  def write_transaction(broker, server, f) do
    snapshot_ref = SnapshotRegistry.new_ref(broker)

    try do
      with {:ok, tx} <- start_transaction(broker, server, snapshot_ref),
           {:ok, tx, reply} <- run_transaction(server, tx, f),
           :ok <- commit_transaction(server, tx) do
        reply
      end
    after
      SnapshotRegistry.unregister_snapshot(broker, snapshot_ref)
      GenServer.cast(server, :try_clean_up)
    end
  end

  @spec read_transaction(:ets.table(), GenServer.server(), :ets.table(), (Goblin.Tx.t() -> any())) ::
          any()
  def read_transaction(broker, server, mem_tables, f) do
    snapshot_ref = SnapshotRegistry.new_ref(broker)

    try do
      seq = MemTables.get_sequence(mem_tables)
      max_level_key = SnapshotRegistry.register_snapshot(broker, snapshot_ref)
      tx = ReadTx.new(broker, snapshot_ref, seq, max_level_key)
      f.(tx)
    after
      SnapshotRegistry.unregister_snapshot(broker, snapshot_ref)
      GenServer.cast(server, :try_clean_up)
    end
  end

  @spec select(:ets.table(), GenServer.server(), :ets.table(), keyword()) ::
          Enumerable.t(Goblin.pair())
  def select(broker, server, mem_tables, opts) do
    min = opts[:min]
    max = opts[:max]
    tag = opts[:tag]

    {min, max} =
      cond do
        is_nil(tag) -> {min, max}
        is_nil(min) and is_nil(max) -> {min, max}
        is_nil(max) -> {{:"$goblin_tag", tag, min}, max}
        is_nil(min) -> {min, {:"$goblin_tag", tag, max}}
        true -> {{:"$goblin_tag", tag, min}, {:"$goblin_tag", tag, max}}
      end

    snapshot_ref = SnapshotRegistry.new_ref(broker)

    Goblin.Iterator.k_merge_stream(
      fn ->
        seq = MemTables.get_sequence(mem_tables)
        SnapshotRegistry.register_snapshot(broker, snapshot_ref)

        SnapshotRegistry.filter_tables(broker, snapshot_ref)
        |> Enum.map(&Goblin.Queryable.stream(&1, min, max, seq))
      end,
      after: fn ->
        SnapshotRegistry.unregister_snapshot(broker, snapshot_ref)
        GenServer.cast(server, :try_clean_up)
      end,
      min: min,
      max: max
    )
    |> Stream.flat_map(fn
      {{:"$goblin_tag", ^tag, key}, _seq, value} ->
        [{key, value}]

      {{:"$goblin_tag", _tag, _key}, _seq, _value} when is_nil(tag) ->
        []

      {key, _seq, value} when is_nil(tag) ->
        [{key, value}]

      _ ->
        []
    end)
  end

  @spec register_table(
          :ets.table(),
          term(),
          Goblin.level_key(),
          Goblin.Queryable.t(),
          (Goblin.Queryable.t() -> :ok)
        ) ::
          :ok
  def register_table(broker, id, level_key, table, delete_callback) do
    SnapshotRegistry.add_table(broker, id, level_key, table, delete_callback)
  end

  @spec soft_delete(:ets.table(), GenServer.server(), term()) :: :ok
  def soft_delete(broker, server, id) do
    SnapshotRegistry.soft_delete(broker, id)
    GenServer.cast(server, :try_clean_up)
  end

  @spec inc_ready(:ets.table()) :: :ok
  def inc_ready(broker), do: SnapshotRegistry.inc_ready(broker)

  @spec deinc_ready(:ets.table()) :: :ok
  def deinc_ready(broker), do: SnapshotRegistry.deinc_ready(broker)

  @impl GenServer
  def init(args) do
    snapshot_registry = SnapshotRegistry.new(args[:name])

    {:ok,
     %__MODULE__{
       snapshot_registry: snapshot_registry,
       mem_tables: args[:mem_tables],
       pub_sub: args[:pub_sub],
       registry: args[:registry]
     }}
  end

  @impl GenServer
  def handle_call(:start_transaction, {pid, _ref}, %{writer: nil} = state) do
    monitor_ref = Process.monitor(pid)
    seq = MemTables.get_sequence(state.mem_tables)
    {:reply, {:ok, seq}, %{state | writer: {pid, monitor_ref}}}
  end

  def handle_call(:start_transaction, {pid, _ref}, %{writer: {pid, _monitor_ref}} = state) do
    {:reply, {:error, :already_in_tx}, state}
  end

  def handle_call(:start_transaction, from, state) do
    write_queue = :queue.in(from, state.write_queue)
    {:noreply, %{state | write_queue: write_queue}}
  end

  def handle_call({:commit_transaction, tx}, {pid, _ref}, %{writer: {pid, monitor_ref}} = state) do
    Process.demonitor(monitor_ref)

    %{writes: writes, seq: seq} = tx
    %{pub_sub: pub_sub, mem_tables: mem_tables, registry: registry} = state

    case MemTables.write(via(registry, mem_tables), seq, writes) do
      :ok ->
        publish_commit(pub_sub, writes)
        {:reply, :ok, %{state | writer: nil}, {:continue, :dequeue_writer}}

      {:error, reason} = error ->
        {:stop, reason, error, state}
    end
  end

  def handle_call({:commit_transaction, _tx}, _from, state) do
    {:reply, {:error, :not_writer}, state}
  end

  def handle_call(:abort_transaction, {pid, _ref}, %{writer: {pid, _monitor_ref}} = state) do
    {:reply, :ok, %{state | writer: nil}, {:continue, :dequeue_writer}}
  end

  def handle_call(:abort_transaction, _from, state) do
    {:reply, {:error, :not_writer}, state}
  end

  @impl GenServer
  def handle_continue(:dequeue_writer, state) do
    case :queue.out(state.write_queue) do
      {:empty, _write_queue} ->
        {:noreply, state}

      {{:value, {pid, _ref} = from}, write_queue} ->
        monitor_ref = Process.monitor(pid)
        seq = MemTables.get_sequence(state.mem_tables)
        GenServer.reply(from, {:ok, seq})
        {:noreply, %{state | write_queue: write_queue, writer: {pid, monitor_ref}}}
    end
  end

  @impl GenServer
  def handle_cast(:try_clean_up, state) do
    case SnapshotRegistry.hard_delete(state.snapshot_registry) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, _, pid, _}, %{writer: {pid, monitor_ref}} = state) do
    {:noreply, %{state | writer: nil}, {:continue, :dequeue_writer}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp start_transaction(broker, server, snapshot_ref) do
    with {:ok, seq} <- GenServer.call(server, :start_transaction) do
      max_level_key = SnapshotRegistry.register_snapshot(broker, snapshot_ref)
      tx = WriteTx.new(broker, snapshot_ref, seq, max_level_key)
      {:ok, tx}
    end
  end

  defp run_transaction(broker, tx, f) do
    case f.(tx) do
      {:commit, tx, reply} ->
        {:ok, %{tx | writes: Enum.reverse(tx.writes)}, reply}

      :abort ->
        abort_transaction(broker)
        {:error, :aborted}

      _ ->
        abort_transaction(broker)
        raise "Invalid return from transaction"
    end
  end

  defp commit_transaction(broker, tx) do
    GenServer.call(broker, {:commit_transaction, tx})
  end

  defp abort_transaction(broker) do
    GenServer.call(broker, :abort_transaction)
  end

  defp publish_commit(pub_sub, writes) do
    Task.start(fn ->
      writes =
        Enum.map(writes, fn
          {:put, _seq, {:"$goblin_tag", tag, key}, value} -> {:put, tag, key, value}
          {:put, _seq, key, value} -> {:put, key, value}
          {:remove, _seq, {:"$goblin_tag", tag, key}} -> {:remove, tag, key}
          {:remove, _seq, key} -> {:remove, key}
        end)

      PubSub.publish(pub_sub, writes)
    end)
  end
end
