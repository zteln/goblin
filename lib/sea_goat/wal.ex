defmodule SeaGoat.WAL do
  @moduledoc """
  Write-ahead log.
  Batches write operations within an interval.
  Default sync interval: 200.
  """
  use GenServer

  @default_sync_interval 200
  @wal_name :sea_goat_wal
  @dump_wal_name :dump_sea_goat_wal

  defstruct [
    :log,
    :sync_interval,
    :last_sync,
    :current_file,
    batch: [],
    waiting_for_sync?: false
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.take(opts, [:sync_interval]), name: opts[:name])
  end

  def sync(wal) do
    GenServer.call(wal, :sync_now)
  end

  def append(wal, term) do
    GenServer.call(wal, {:append, term})
  end

  def append_batch(wal, batch) do
    GenServer.call(wal, {:append_batch, batch})
  end

  def rotate(wal, path, prepend, append) do
    GenServer.call(wal, {:rotate, path, prepend, append})
  end

  def replay(wal, file) do
    GenServer.call(wal, {:replay, file})
  end

  def dump(wal, path, dump) do
    GenServer.call(wal, {:dump, path, dump})
  end

  def current_file(wal) do
    GenServer.call(wal, :current_file)
  end

  @impl GenServer
  def init(opts) do
    {:ok,
     %__MODULE__{
       sync_interval: opts[:sync_interval] || @default_sync_interval,
       last_sync: now()
     }}
  end

  @impl GenServer
  def handle_call(:sync_now, _from, state) do
    append_and_sync_log(state.log, Enum.reverse(state.batch))
    {:reply, :ok, %{state | last_sync: now(), batch: []}}
  end

  def handle_call({:append, term}, _from, state) do
    {:reply, :ok, %{state | batch: [term | state.batch]}, {:continue, :sync}}
  end

  def handle_call({:append_batch, batch}, _from, state) do
    {:reply, :ok, %{state | batch: batch ++ state.batch}, {:continue, :sync}}
  end

  def handle_call({:rotate, new_file, prepend, append}, _from, state) do
    logs = [prepend | Enum.reverse([append | state.batch])]

    with :ok <- append_and_sync_log(state.log, logs),
         :ok <- close_log(state.log),
         {:ok, log} <- open_log(new_file, @wal_name) do
      {:reply, :ok,
       %{
         state
         | log: log,
           batch: [],
           current_file: new_file,
           last_sync: now()
       }}
    else
      {:error, _reason} = e ->
        {:stop, e, state}
    end
  end

  def handle_call({:dump, path, dump}, _from, state) do
    reply =
      with {:ok, log} <- open_log(path, @dump_wal_name),
           :ok <- append_and_sync_log(log, dump) do
        close_log(log)
      end

    {:reply, reply, state}
  end

  def handle_call({:replay, file}, _from, state) do
    case open_log(file, @wal_name) do
      {:ok, log} ->
        state.log && close_log(state.log)
        logs = collect_logs(log)
        {:reply, {:ok, logs}, %{state | log: log, current_file: file}}

      {:error, _reason} ->
        {:reply, {:error, :not_a_log}, state}
    end
  end

  def handle_call(:current_file, _from, state) do
    {:reply, state.current_file, state}
  end

  @impl GenServer
  def handle_continue(:sync, state) do
    now = now()

    if now - state.last_sync >= state.sync_interval do
      append_and_sync_log(state.log, Enum.reverse(state.batch))
      {:noreply, %{state | last_sync: now, batch: []}}
    else
      state = send_sync(state)
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:sync, state) do
    {:noreply, %{state | waiting_for_sync?: false}, {:continue, :sync}}
  end

  defp open_log(file, name, opts \\ []) do
    opts = [name: name, file: ~c"#{file}"] |> Keyword.merge(opts)

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      {:error, _reason} = e -> e
    end
  end

  defp append_and_sync_log(log, batch) do
    :disk_log.log_terms(log, batch)
    :disk_log.sync(log)
  end

  defp close_log(log), do: :disk_log.close(log)

  defp send_sync(state) do
    if state.waiting_for_sync? do
      state
    else
      Process.send_after(self(), :sync, state.sync_interval)
      %{state | waiting_for_sync?: true}
    end
  end

  defp collect_logs(log, acc \\ [], continuation \\ :start) do
    case :disk_log.chunk(log, continuation) do
      :eof ->
        acc

      {continuation, chunk} ->
        collect_logs(log, acc ++ chunk, continuation)
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
