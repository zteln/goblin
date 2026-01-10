defmodule Goblin.WAL do
  @moduledoc false
  use GenServer

  alias Goblin.Manifest
  alias Goblin.Cleaner

  @log_file "wal.goblin"

  defstruct [
    :wal,
    :name,
    :ro_name,
    :dir,
    :manifest_server,
    :cleaner_server,
    rotations: [],
    cleaning: %{}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      local_name: opts[:local_name] || name,
      db_dir: opts[:db_dir],
      manifest_server: opts[:manifest_server],
      cleaner_server: opts[:cleaner_server]
    ]

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec append(Goblin.server(), [term()]) :: :ok | {:error, term()}
  def append(wal, logs) do
    GenServer.call(wal, {:append, logs})
  end

  @spec rotate(Goblin.server()) :: {:ok, Path.t(), Path.t()}
  def rotate(wal) do
    GenServer.call(wal, :rotate)
  end

  @spec clean(Goblin.server(), Path.t()) :: :ok | {:error, term()}
  def clean(wal, rotation) do
    GenServer.call(wal, {:clean, rotation})
  end

  @spec get_log_streams(Goblin.server()) :: [{Path.t(), Enumerable.t()}]
  def get_log_streams(wal) do
    GenServer.call(wal, :get_log_streams)
  end

  @impl GenServer
  def init(args) do
    name = args[:local_name]
    ro_name = Module.concat(name, RO)
    db_dir = args[:db_dir]
    manifest_server = args[:manifest_server]
    cleaner_server = args[:cleaner_server]

    {{_count, file} = wal, rotations} =
      case Manifest.snapshot(manifest_server, [:wal, :wal_rotations]) do
        %{wal: nil, wal_rotations: []} ->
          {_, file} = wal = new_wal(db_dir, [])
          Manifest.log_wal(manifest_server, file)
          {wal, []}

        %{wal: wal, wal_rotations: rotations} ->
          {parse_file(wal), Enum.map(rotations, &parse_file/1)}
      end

    case open_log(file, name) do
      :ok ->
        {:ok,
         %__MODULE__{
           name: name,
           ro_name: ro_name,
           dir: db_dir,
           wal: wal,
           rotations: rotations,
           manifest_server: manifest_server,
           cleaner_server: cleaner_server
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:append, logs}, _from, state) do
    case append_and_sync_log(state.name, logs) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_call(:rotate, _from, state) do
    {_, rotation_file} = wal = state.wal
    rotations = [wal | state.rotations]
    {_, file} = new_wal = new_wal(state.dir, rotations)

    case rotate_log(state.name, file) do
      :ok ->
        state = %{state | wal: new_wal, rotations: rotations}
        {:reply, {:ok, rotation_file, file}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:clean, rotation}, _from, state) do
    Cleaner.clean(state.cleaner_server, [rotation])
    rotations = Enum.reject(state.rotations, fn {_, file} -> file == rotation end)
    {:reply, :ok, %{state | rotations: rotations}}
  end

  def handle_call(:get_log_streams, _from, state) do
    {:reply, recover_logs(state), state}
  end

  defp rotate_log(name, file) do
    with :ok <- close_log(name) do
      open_log(file, name)
    end
  end

  defp open_log(file, name, opts \\ []) do
    opts = [name: name, file: ~c"#{file}"] |> Keyword.merge(opts)

    case :disk_log.open(opts) do
      {:ok, _log} -> :ok
      {:repaired, _log, _recovered, _bad_bytes} -> :ok
      {:error, _reason} = e -> e
    end
  end

  defp close_log(name), do: :disk_log.close(name)

  defp append_and_sync_log(name, logs) do
    with :ok <- :disk_log.log_terms(name, logs) do
      :disk_log.sync(name)
    end
  end

  defp recover_logs(state) do
    %{
      ro_name: ro_name,
      rotations: rotations,
      wal: wal
    } = state

    [wal | rotations]
    |> Enum.map(&elem(&1, 1))
    |> Enum.map(&{&1, stream_logs(&1, ro_name)})
    |> Enum.reverse()
  end

  defp stream_logs(log_file, name) do
    Stream.resource(
      fn ->
        case open_log(log_file, name, mode: :read_only) do
          :ok -> {name, :start}
          {:error, _reason} -> raise "Failed to open WAL log"
        end
      end,
      fn {name, continuation} ->
        case :disk_log.chunk(name, continuation) do
          {:error, _reason} -> raise "Failed to stream WAL log"
          :eof -> {:halt, name}
          {continuation, chunk} -> {chunk, {name, continuation}}
        end
      end,
      fn name ->
        case close_log(name) do
          :ok -> :ok
          {:error, _reason} -> raise "Failed to close WAL log"
        end
      end
    )
  end

  defp parse_file(file) do
    [_, _, count_str] =
      file
      |> Path.basename()
      |> String.split(".", trim: true)

    count = String.to_integer(count_str)
    {count, file}
  end

  defp new_wal(dir, rotations) do
    new_count =
      rotations
      |> List.keysort(0)
      |> Enum.reduce_while(0, fn
        {count, _file}, acc when acc < count ->
          if File.exists?(Path.join(dir, "#{@log_file}.#{acc}")) do
            {:cont, acc + 1}
          else
            {:halt, acc}
          end

        _, acc ->
          {:cont, acc + 1}
      end)

    {new_count, Path.join(dir, "#{@log_file}.#{new_count}")}
  end
end
