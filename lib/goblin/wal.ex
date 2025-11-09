defmodule Goblin.WAL do
  @moduledoc false
  use GenServer

  @log_file "wal.goblin"
  @rotated_log_suffix ".rot"

  @type wal :: module() | {:via, Registry, {module(), module()}}

  defstruct [
    :log,
    :file,
    :name,
    :ro_name,
    rotated_files: []
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]
    args = [local_name: opts[:local_name] || name, db_dir: opts[:db_dir]]
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec append(wal(), [term()]) :: :ok
  def append(wal, buffer) do
    GenServer.call(wal, {:append, buffer})
  end

  @spec rotate(wal()) :: Goblin.db_file()
  def rotate(wal) do
    GenServer.call(wal, :rotate)
  end

  @spec clean(wal(), Goblin.db_file()) :: :ok | {:error, term()}
  def clean(wal, rotated_file) do
    GenServer.call(wal, {:clean, rotated_file})
  end

  @spec recover(wal()) ::
          {:ok, [{Goblin.db_file() | nil, [term()]}]} | {:error, term()}
  def recover(wal) do
    GenServer.call(wal, :recover)
  end

  @impl GenServer
  def init(args) do
    name = args[:local_name]
    ro_name = Module.concat(name, RO)
    file = Path.join(args[:db_dir], @log_file)

    case open_log(file, name) do
      {:ok, log} ->
        {:ok,
         %__MODULE__{
           name: name,
           ro_name: ro_name,
           file: file,
           log: log
         }, {:continue, :get_rotated_files}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:append, buffer}, _from, state) do
    case append_and_sync_log(state.log, Enum.reverse(buffer)) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_call(:rotate, _from, state) do
    case rotate_log(state.name, state.file, state.log) do
      {:ok, log, rotated_file} ->
        state = %{
          state
          | log: log,
            rotated_files: [rotated_file | state.rotated_files]
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
    {:reply, recover_writes(state.log, state.ro_name, state.file), state}
  end

  @impl GenServer
  def handle_continue(:get_rotated_files, state) do
    state = %{state | rotated_files: rotated_files(state.file)}
    {:noreply, state}
  end

  defp rotate_log(name, file, log) do
    with :ok <- close_log(log),
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

  defp recover_writes(log, ro_log, file) do
    with {:ok, rotated_writes} <- recover_rotated_writes(file, ro_log),
         {:ok, writes} <- recover_logs(log) do
      {:ok, rotated_writes ++ [{nil, writes}]}
    end
  end

  defp recover_rotated_writes(file, ro_log) do
    file
    |> rotated_files()
    |> Enum.reduce_while({:ok, []}, fn rotated_file, {:ok, acc} ->
      case get_writes(rotated_file, ro_log) do
        {:ok, writes} -> {:cont, {:ok, [{rotated_file, writes} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp get_writes(file, ro_log) do
    with {:ok, log} <- open_log(file, ro_log, mode: :read_only),
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

    File.ls!(dir)
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
