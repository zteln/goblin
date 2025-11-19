defmodule Goblin.WAL do
  @moduledoc false
  use GenServer

  @log_file "wal.goblin"
  @retry_attempts 5

  @typep wal :: module() | {:via, Registry, {module(), module()}}

  defstruct [
    :wal,
    :name,
    :ro_name,
    :dir,
    :manifest,
    :reader,
    :task_mod,
    :task_sup,
    rotations: [],
    cleaning: %{}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      local_name: opts[:local_name] || name,
      db_dir: opts[:db_dir],
      manifest: opts[:manifest],
      reader: opts[:reader],
      task_mod: opts[:task_mod],
      task_sup: opts[:task_sup]
    ]

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
  def clean(wal, rotation) do
    GenServer.call(wal, {:clean, rotation})
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
    db_dir = args[:db_dir]
    manifest = args[:manifest]

    {{_count, file} = wal, rotations} =
      case Goblin.Manifest.get_version(manifest, [:wal, :wal_rotations]) do
        %{wal: nil, wal_rotations: []} ->
          {_, file} = wal = new_wal(db_dir, [])
          Goblin.Manifest.log_wal(manifest, file)
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
           manifest: manifest,
           reader: args[:reader],
           task_mod: args[:task_mod] || Task.Supervisor,
           task_sup: args[:task_sup]
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:append, buffer}, _from, state) do
    case append_and_sync_log(state.name, Enum.reverse(buffer)) do
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
    ref = clean_up(state, rotation)
    cleaning = Map.put(state.cleaning, ref, {rotation, @retry_attempts})
    state = %{state | cleaning: cleaning}
    {:reply, :ok, state}
  end

  def handle_call(:recover, _from, state) do
    {:reply, recover_writes(state), state}
  end

  @impl GenServer
  def handle_info({ref, {:ok, :cleaned}}, state) do
    {cleaned, cleaning} = Map.pop(state.cleaning, ref)

    case cleaned do
      nil ->
        state = %{state | cleaning: cleaning}
        {:noreply, state}

      {rotation, _retries} ->
        rotations = Enum.reject(state.rotations, fn {_, file} -> file == rotation end)
        state = %{state | rotations: rotations, cleaning: cleaning}
        {:noreply, state}
    end
  end

  def handle_info({ref, {:error, _reason}}, state) do
    case retry_clean_up(state, ref) do
      {:ok, state} -> {:noreply, state}
      {:error, _reason} = error -> {:stop, error, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    case retry_clean_up(state, ref) do
      {:ok, state} -> {:noreply, state}
      {:error, _reason} = error -> {:stop, error, state}
    end
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

  defp clean_up(state, file) do
    %{
      reader: reader,
      task_mod: task_mod,
      task_sup: task_sup
    } = state

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        with :ok <- Goblin.Reader.empty?(reader),
             :ok <- File.rm(file) do
          {:ok, :cleaned}
        end
      end)

    ref
  end

  defp retry_clean_up(state, ref) do
    {to_clean, cleaning} = Map.pop(state.cleaning, ref)

    case to_clean do
      nil ->
        state = %{state | cleaning: cleaning}
        {:ok, state}

      {_file, 0} ->
        {:error, :failed_to_clean_wal}

      {file, retries} ->
        ref = clean_up(state, file)
        cleaning = Map.put(cleaning, ref, {file, retries - 1})
        state = %{state | cleaning: cleaning}
        {:ok, state}
    end
  end

  defp append_and_sync_log(name, buffer) do
    with :ok <- :disk_log.log_terms(name, buffer) do
      :disk_log.sync(name)
    end
  end

  defp recover_writes(state) do
    %{
      name: name,
      ro_name: ro_name,
      rotations: rotations
    } = state

    with {:ok, rotated_writes} <- recover_rotated_writes(rotations, ro_name),
         {:ok, writes} <- recover_logs(name) do
      {:ok, rotated_writes ++ [{nil, writes}]}
    end
  end

  defp recover_rotated_writes(rotations, ro_name) do
    Enum.reduce_while(rotations, {:ok, []}, fn {_count, rotation}, {:ok, acc} ->
      case get_writes(rotation, ro_name) do
        {:ok, writes} -> {:cont, {:ok, [{rotation, writes} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp get_writes(file, ro_name) do
    with :ok <- open_log(file, ro_name, mode: :read_only),
         {:ok, writes} <- recover_logs(ro_name),
         :ok <- close_log(ro_name) do
      {:ok, writes}
    end
  end

  defp recover_logs(name, continuation \\ :start, acc \\ []) do
    case :disk_log.chunk(name, continuation) do
      {:error, _reason} ->
        {:error, :failed_to_recover}

      :eof ->
        {:ok, acc}

      {continuation, chunk} ->
        acc = acc ++ chunk
        recover_logs(name, continuation, acc)
    end
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
      Enum.reduce_while(rotations, 0, fn
        {count, _file}, acc when acc < count -> {:halt, acc}
        _, acc -> {:cont, acc + 1}
      end)

    {new_count, Path.join(dir, "#{@log_file}.#{new_count}")}
  end
end
