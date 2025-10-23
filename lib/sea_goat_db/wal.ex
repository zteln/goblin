defmodule SeaGoatDB.WAL do
  @moduledoc false
  use GenServer

  @default_sync_interval 200
  @log_name :sea_goat_db_wal
  @ro_log_name :sea_goat_db_wal_ro
  @log_file "wal.seagoat"
  @rotated_log_suffix ".rot"

  defstruct [
    :log,
    :sync_interval,
    :last_sync,
    :file,
    :name,
    rotated_files: [],
    buffer: [],
    waiting_for_sync?: false
  ]

  def sync(wal) do
    GenServer.call(wal, :sync_now)
  end

  def append(wal, buffer) do
    GenServer.call(wal, {:append, buffer})
  end

  def rotate(wal) do
    GenServer.call(wal, :rotate)
  end

  def clean(wal, rotated_file) do
    GenServer.call(wal, {:clean, rotated_file})
  end

  def recover(wal) do
    GenServer.call(wal, :recover)
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:sync_interval, :log_name, :db_dir])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @impl GenServer
  def init(args) do
    name = args[:log_name] || @log_name
    file = Path.join(args[:db_dir], @log_file)
    sync_interval = (args[:sync_interval] || @default_sync_interval) * Integer.pow(10, 6)

    case open_log(file, name) do
      {:ok, log} ->
        {:ok,
         %__MODULE__{
           name: name,
           file: file,
           log: log,
           sync_interval: sync_interval,
           last_sync: now()
         }, {:continue, :get_rotated_files}}

      {:error, _} = error ->
        {:stop, error}
    end
  end

  @impl GenServer
  def handle_call(:sync_now, _from, state) do
    append_and_sync_log(state.log, Enum.reverse(state.buffer))
    {:reply, :ok, %{state | last_sync: now(), buffer: []}}
  end

  def handle_call({:append, buffer}, _from, state) do
    buffer = buffer ++ state.buffer
    state = %{state | buffer: buffer}
    {:reply, :ok, state, {:continue, :sync}}
  end

  def handle_call(:rotate, _from, state) do
    case rotate_log(state.name, state.file, state.log, state.buffer) do
      {:ok, log, rotated_file} ->
        state = %{
          state
          | log: log,
            rotated_files: [rotated_file | state.rotated_files],
            buffer: [],
            last_sync: now()
        }

        {:reply, {:ok, rotated_file}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:clean, rotated_file}, _from, state) do
    case rm_log(rotated_file) do
      :ok ->
        rotated_files = Enum.reject(state.rotated_files, &(&1 == rotated_file))
        state = %{state | rotated_files: rotated_files}
        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:recover, _from, state) do
    {:reply, recover_writes(state.log, state.file), state}
  end

  @impl GenServer
  def handle_continue(:get_rotated_files, state) do
    state = %{state | rotated_files: rotated_files(state.file)}
    {:noreply, state}
  end

  def handle_continue(:sync, state) do
    now = now()

    if now - state.last_sync >= state.sync_interval do
      append_and_sync_log(state.log, Enum.reverse(state.buffer))
      {:noreply, %{state | last_sync: now, buffer: []}}
    else
      state = send_sync(state)
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:sync, state) do
    {:noreply, %{state | waiting_for_sync?: false}, {:continue, :sync}}
  end

  defp rotate_log(name, file, log, buffer) do
    with :ok <- append_and_sync_log(log, Enum.reverse(buffer)),
         :ok <- close_log(log),
         {:ok, rotated_file} <- mv_log(file),
         {:ok, log} <- open_log(file, name) do
      {:ok, log, rotated_file}
    end
  end

  defp open_log(file, name, opts \\ []) do
    opts = [name: name, file: ~c"#{file}"] |> Keyword.merge(opts)

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      {:error, _reason} = e -> e
    end
  end

  defp close_log(log), do: :disk_log.close(log)

  defp mv_log(file) do
    rotated_file = rotated_file(file)

    with :ok <- File.rename(file, rotated_file) do
      {:ok, rotated_file}
    end
  end

  defp rm_log(file) do
    File.rm(file)
  end

  defp append_and_sync_log(log, buffer) do
    with :ok <- :disk_log.log_terms(log, buffer) do
      :disk_log.sync(log)
    end
  end

  defp send_sync(state) do
    if state.waiting_for_sync? do
      state
    else
      Process.send_after(self(), :sync, state.sync_interval)
      %{state | waiting_for_sync?: true}
    end
  end

  defp recover_writes(log, file) do
    with {:ok, rotated_writes} <- recover_rotated_writes(file),
         {:ok, writes} <- recover_logs(log) do
      {:ok, rotated_writes ++ [{nil, writes}]}
    end
  end

  defp recover_rotated_writes(file) do
    file
    |> rotated_files()
    |> Enum.reduce_while({:ok, []}, fn rotated_file, {:ok, acc} ->
      case get_writes(rotated_file) do
        {:ok, writes} -> {:cont, {:ok, [{rotated_file, writes} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp get_writes(file) do
    with {:ok, log} <- open_log(file, @ro_log_name, mode: :read_only),
         {:ok, writes} <- recover_logs(log),
         :ok <- close_log(log) do
      {:ok, writes}
    end
  end

  defp recover_logs(log, continuation \\ :start, acc \\ []) do
    case :disk_log.chunk(log, continuation) do
      {:error, _reason} ->
        {:error, :failed_to_recover}

      :eof ->
        {:ok, acc}

      {continuation, chunk} ->
        acc = acc ++ chunk
        recover_logs(log, continuation, acc)
    end
  end

  defp now, do: System.monotonic_time(:nanosecond)
  defp rotated_file(file), do: "#{file}#{@rotated_log_suffix}.#{abs(now())}"

  defp rotated_files(file) do
    filename = Path.basename(file)
    dir = Path.dirname(file)

    dir
    |> File.ls!()
    |> Enum.filter(&String.starts_with?(&1, filename <> @rotated_log_suffix <> "."))
    |> Enum.flat_map(fn file ->
      case String.split(Path.basename(file), ".") do
        [_, _, _, time] -> [{time, Path.join(dir, file)}]
        _ -> []
      end
    end)
    |> List.keysort(0, :asc)
    |> Enum.map(&elem(&1, 1))
  end
end
