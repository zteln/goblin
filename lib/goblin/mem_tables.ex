defmodule Goblin.MemTables do
  @moduledoc false
  use GenServer

  alias Goblin.{
    Broker,
    DiskTables,
    Manifest
  }

  alias Goblin.MemTables.{
    MemTable,
    WAL
  }

  import Goblin.Registry, only: [via: 2]

  @flush_level_key 0
  @mem_level_key -1

  defstruct [
    :data_dir,
    :mem_table,
    :seq_table,
    :wal,
    :mem_limit,
    :flushing,
    :broker,
    :manifest,
    :disk_tables,
    :registry,
    seq: 0,
    flush_queue: :queue.new(),
    wal_rotations: []
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name] || __MODULE__

    args = [
      name: name,
      data_dir: opts[:data_dir],
      mem_limit: opts[:mem_limit],
      manifest: opts[:manifest],
      broker: opts[:broker],
      disk_tables: opts[:disk_tables],
      registry: opts[:registry]
    ]

    GenServer.start_link(__MODULE__, args, name: via(opts[:registry], name))
  end

  @doc "Writes to both the WAL and current MemTable."
  @spec write(GenServer.server(), Goblin.seq_no(), [Goblin.write_term()]) ::
          :ok | {:error, term()}
  def write(server, seq, writes) do
    GenServer.call(server, {:write, seq, writes})
  end

  @doc "Gets the current sequence number."
  @spec get_sequence(:ets.table()) :: non_neg_integer()
  def get_sequence(mem_tables) do
    case :ets.lookup(mem_tables, :seq) do
      [] -> 0
      [{_, seq}] -> seq
    end
  end

  @doc "Returns a boolean indicating whether there is an ongoing flush or not."
  @spec flushing?(GenServer.server()) :: boolean()
  def flushing?(server) do
    GenServer.call(server, :flushing?)
  end

  @impl GenServer
  def init(args) do
    seq_table = :ets.new(args[:name], [:named_table])

    {:ok,
     %__MODULE__{
       data_dir: args[:data_dir],
       seq_table: seq_table,
       mem_limit: args[:mem_limit],
       broker: args[:broker],
       disk_tables: via(args[:registry], args[:disk_tables]),
       manifest: via(args[:registry], args[:manifest]),
       registry: args[:registry]
     }, {:continue, {:recover, args[:name]}}}
  end

  @impl GenServer
  def handle_call({:write, new_seq, writes}, _from, state) do
    with :ok <- WAL.append(state.wal, writes),
         :ok <- Manifest.update(state.manifest, seq: new_seq) do
      Enum.each(writes, &apply_write(state.mem_table, &1))
      :ets.insert(state.seq_table, {:seq, new_seq})
      {:reply, :ok, %{state | seq: new_seq}, {:continue, :maybe_flush}}
    else
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  def handle_call(:flushing?, _from, %{flushing: nil} = state),
    do: {:reply, false, state}

  def handle_call(:flushing?, _from, state),
    do: {:reply, true, state}

  @impl GenServer
  def handle_info({ref, {:ok, :flushed}}, %{flushing: ref} = state) do
    {:noreply, %{state | flushing: nil}, {:continue, :next_flush}}
  end

  def handle_info({ref, {:error, reason}}, %{flushing: ref} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{flushing: ref} = state) do
    {:stop, reason, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl GenServer
  def handle_continue(:maybe_flush, state) do
    case flush?(state) do
      true ->
        case rotate_and_flush(state) do
          {:ok, state} -> {:noreply, state}
          {:error, reason} -> {:stop, reason, state}
        end

      false ->
        {:noreply, state}
    end
  end

  def handle_continue(:next_flush, state) do
    state = dequeue_flush(state)
    {:noreply, state}
  end

  def handle_continue({:recover, name}, state) do
    %{wal: wal, wal_rotations: wal_rotations, seq: seq} =
      Manifest.snapshot(state.manifest, [:wal, :wal_rotations, :seq])

    wals = Enum.reverse(List.wrap(wal) ++ wal_rotations)

    case replay_wal_logs(%{state | seq: seq}, name, wals) do
      {:ok, state} ->
        Broker.inc_ready(state.broker)
        :ets.insert(state.seq_table, {:seq, seq})
        {:noreply, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    Broker.deinc_ready(state.broker)
    :ok
  end

  defp replay_wal_logs(%{wal: nil} = state, name, []) do
    wal_file = WAL.filename(state.data_dir)
    mem_table = MemTable.new(wal_file)

    Broker.register_table(
      state.broker,
      mem_table.wal_name,
      @mem_level_key,
      mem_table,
      &MemTable.delete/1
    )

    with {:ok, wal} <- WAL.open(name, wal_file),
         Manifest.update(state.manifest, create_wal: wal_file) do
      {:ok, %{state | mem_table: mem_table, wal: wal}}
    end
  end

  defp replay_wal_logs(state, name, [wal_file]) do
    mem_table = MemTable.new(wal_file)

    Broker.register_table(
      state.broker,
      mem_table.wal_name,
      @mem_level_key,
      mem_table,
      &MemTable.delete/1
    )

    with {:ok, wal} <- WAL.open(name, wal_file) do
      state = replay_wal(%{state | mem_table: mem_table, wal: wal})

      case flush?(state) do
        true -> rotate_and_flush(state)
        false -> {:ok, state}
      end
    end
  end

  defp replay_wal_logs(state, name, [wal_file | wal_files]) do
    mem_table = MemTable.new(wal_file)

    Broker.register_table(
      state.broker,
      mem_table.wal_name,
      @mem_level_key,
      mem_table,
      &MemTable.delete/1
    )

    with {:ok, wal} <- WAL.open(name, wal_file, write?: false),
         state <- replay_wal(%{state | mem_table: mem_table, wal: wal}),
         :ok <- WAL.close(wal) do
      broker_pid = via(state.registry, state.broker) |> GenServer.whereis()
      MemTable.give_away(state.mem_table, broker_pid)

      state
      |> enqueue_flush()
      |> replay_wal_logs(name, wal_files)
    end
  end

  defp replay_wal(state) do
    %{seq: max_seq} = state

    state.wal
    |> WAL.stream_log!()
    |> Stream.filter(fn
      {:put, seq, _key, _val} -> seq < max_seq
      {:remove, seq, _key} -> seq < max_seq
    end)
    |> Enum.each(&apply_write(state.mem_table, &1))

    state
  end

  defp rotate_and_flush(state) do
    with :ok <- WAL.close(state.wal),
         {:ok, wal} <- WAL.open(state.wal),
         :ok <-
           Manifest.update(
             state.manifest,
             create_wal: wal.file,
             add_wals: [state.wal.file]
           ) do
      state = enqueue_flush(state)

      broker_pid = via(state.registry, state.broker) |> GenServer.whereis()
      MemTable.give_away(state.mem_table, broker_pid)

      mem_table = MemTable.new(wal.file)

      Broker.register_table(
        state.broker,
        mem_table.wal_name,
        @mem_level_key,
        mem_table,
        &MemTable.delete/1
      )

      {:ok, %{state | mem_table: mem_table, wal: wal}}
    end
  end

  defp flush?(state), do: MemTable.size(state.mem_table) > state.mem_limit

  defp enqueue_flush(%{flushing: nil} = state) do
    flush_ref = start_flush(state, state.mem_table, state.wal)
    %{state | flushing: flush_ref}
  end

  defp enqueue_flush(state) do
    flush_queue = :queue.in({state.mem_table, state.wal}, state.flush_queue)
    %{state | flush_queue: flush_queue}
  end

  defp dequeue_flush(state) do
    case :queue.out(state.flush_queue) do
      {:empty, _flush_queue} ->
        state

      {{:value, {mem_table, wal}}, flush_queue} ->
        flush_ref = start_flush(state, mem_table, wal)
        %{state | flushing: flush_ref, flush_queue: flush_queue}
    end
  end

  defp start_flush(state, mem_table, wal) do
    %{
      disk_tables: disk_tables,
      manifest: manifest,
      broker: broker,
      registry: registry,
      seq: seq
    } = state

    %{ref: ref} =
      Task.async(fn ->
        opts = [
          level_key: @flush_level_key,
          compress?: false
        ]

        stream =
          Goblin.Iterator.linear_stream(fn ->
            Goblin.Queryable.stream(mem_table, nil, nil, seq)
          end)

        with {:ok, new_disk_tables} <- DiskTables.new(disk_tables, stream, opts),
             :ok <-
               Manifest.update(manifest,
                 add_disk_tables: Enum.map(new_disk_tables, & &1.file),
                 remove_wals: [wal.file]
               ),
             :ok <- WAL.delete(wal) do
          Broker.soft_delete(broker, via(registry, broker), mem_table.wal_name)
          {:ok, :flushed}
        end
      end)

    ref
  end

  defp apply_write(mem_table, {:put, seq, key, value}),
    do: MemTable.insert(mem_table, key, seq, value)

  defp apply_write(mem_table, {:remove, seq, key}),
    do: MemTable.remove(mem_table, key, seq)
end
