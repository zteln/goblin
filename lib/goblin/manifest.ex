defmodule Goblin.Manifest do
  @moduledoc false
  use GenServer

  @manifest_file "manifest.goblin"
  @tmp_suffix ".goblin.tmp"
  @manifest_max_size 1024 * 1024
  @retry_attempts 5

  @type version :: %{
          ssts: [Goblin.db_file()],
          wal_rotations: [Goblin.db_file()],
          wal: Goblin.db_file() | nil,
          count: non_neg_integer(),
          seq: Goblin.seq_no()
        }
  @typep manifest :: module() | {:via, Registry, {module(), module()}}

  defstruct [
    :name,
    :file,
    :max_size,
    :version,
    :waiting,
    :task_mod,
    :task_sup,
    size: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      local_name: opts[:local_name] || name,
      db_dir: opts[:db_dir],
      manifest_max_size: opts[:manifest_max_size],
      task_mod: opts[:task_mod],
      task_sup: opts[:task_sup]
    ]

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec log_wal(manifest(), Goblin.db_file()) :: :ok | {:error, term()}
  def log_wal(manifest, wal) do
    GenServer.call(manifest, {:log_edit, {:current_wal, wal}})
  end

  @spec log_rotation(manifest(), Goblin.db_file(), Goblin.db_file()) :: :ok | {:error, term()}
  def log_rotation(manifest, wal_rotation, wal_current) do
    edits = [{:wal_added, wal_rotation}, {:current_wal, wal_current}]
    GenServer.call(manifest, {:log_edit, edits})
  end

  @spec log_flush(manifest(), [Goblin.db_file()], Goblin.db_file()) ::
          :ok | {:error, term()}
  def log_flush(manifest, ssts, wal) do
    added_edits = Enum.map(ssts, &{:sst_added, &1})
    removed_edit = {:wal_removed, wal}
    GenServer.call(manifest, {:log_edit, [removed_edit | added_edits]})
  end

  @spec log_sequence(manifest(), Goblin.seq_no()) :: :ok | {:error, term()}
  def log_sequence(manifest, seq) do
    GenServer.call(manifest, {:log_edit, {:seq, seq}})
  end

  @spec log_compaction(manifest(), [Goblin.db_file()], [Goblin.db_file()]) ::
          :ok | {:error, term()}
  def log_compaction(manifest, removed_ssts, added_ssts) do
    added_edits = Enum.map(added_ssts, &{:sst_added, &1})
    removed_edits = Enum.map(removed_ssts, &{:sst_removed, &1})
    GenServer.call(manifest, {:log_edit, added_edits ++ removed_edits})
  end

  @spec get_version(manifest(), [:ssts | :wal_rotations | :wal | :count | :seq]) :: version()
  def get_version(manifest, keys \\ []) do
    GenServer.call(manifest, {:get_version, keys})
  end

  @spec export(manifest(), Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def export(manifest, dir) do
    GenServer.call(manifest, {:export, dir}, :infinity)
  end

  @impl GenServer
  def init(args) do
    name = args[:local_name]
    db_dir = args[:db_dir]
    file = Path.join(db_dir, @manifest_file)
    max_size = args[:manifest_max_size] || @manifest_max_size

    if File.exists?(rotated_file(file)) do
      recover_rotated_manifest(file)
    end

    with :ok <- open_manifest(name, file),
         {:ok, version} <- fetch_version(name),
         :ok <- clean_up(db_dir, version) do
      {:ok,
       %__MODULE__{
         name: name,
         file: file,
         max_size: max_size,
         version: version,
         task_mod: args[:task_mod] || Task.Supervisor,
         task_sup: args[:task_sup]
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:log_edit, edit}, _from, state) do
    case append_to_manifest(state.name, edit) do
      {:ok, size} ->
        version = apply_edit(state.version, edit)
        {:reply, :ok, %{state | size: state.size + size, version: version}, {:continue, :rotate}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get_version, keys}, _from, state) do
    version =
      state.version
      |> Map.take(keys)
      |> Map.replace_lazy(:ssts, &MapSet.to_list/1)
      |> Map.replace_lazy(:wal_rotations, &MapSet.to_list/1)

    {:reply, version, state}
  end

  def handle_call({:export, dir}, from, %{waiting: nil} = state) do
    case start_export(state, dir, from) do
      {:ok, state} ->
        {:noreply, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:export, _dir}, _from, state) do
    {:reply, {:error, :already_exporting}, state}
  end

  @impl GenServer
  def handle_continue(:rotate, %{size: size, max_size: max_size} = state) when size >= max_size do
    case rotate(state) do
      {:ok, state} ->
        {:noreply, state}

      error ->
        {:stop, error, state}
    end
  end

  def handle_continue(:rotate, state), do: {:noreply, state}

  @impl GenServer
  def handle_info({ref, {:ok, tar_name}}, %{waiting: {ref, _, _, _}} = state) do
    %{waiting: {_ref, from, _dir, _retry}} = state
    GenServer.reply(from, {:ok, tar_name})
    state = %{state | waiting: nil}
    {:noreply, state}
  end

  def handle_info({ref, {:error, _reason} = error}, %{waiting: {ref, _, _, _}} = state) do
    case retry_export(state) do
      {:ok, state} ->
        {:noreply, state}

      _error ->
        %{waiting: {_, from, _, _}} = state
        GenServer.reply(from, error)
        state = %{state | waiting: nil}
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, %{waiting: {ref, _, _, _}} = state) do
    case retry_export(state) do
      {:ok, state} ->
        {:noreply, state}

      error ->
        %{waiting: {_, from, _, _}} = state
        GenServer.reply(from, error)
        state = %{state | waiting: nil}
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp clean_up(dir, version) do
    rm_tmp_files!(dir)
    rm_orphaned_files!(dir, version)
  end

  defp rm_tmp_files!(dir) do
    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.filter(&String.ends_with?(&1, @tmp_suffix))
    |> Enum.each(&File.rm!/1)
  end

  defp rm_orphaned_files!(dir, version) do
    %{ssts: ssts, wal_rotations: wal_rotations, wal: wal} = version

    tracked_files = [wal | MapSet.to_list(ssts) ++ MapSet.to_list(wal_rotations)]

    dir
    |> File.ls!()
    |> Enum.reject(&(&1 == @manifest_file))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.reject(&(&1 in tracked_files))
    |> Enum.each(&File.rm!/1)
  end

  defp rotate(state) do
    with :ok <- close_manifest(state.name),
         :ok <- File.rename(state.file, rotated_file(state.file)),
         :ok <- open_manifest(state.name, state.file),
         {:ok, size} <- append_to_manifest(state.name, {:snapshot, state.version}),
         :ok <- File.rm(rotated_file(state.file)) do
      {:ok, %{state | size: size}}
    end
  end

  defp open_manifest(name, file) do
    opts = [name: name, file: ~c"#{file}"]

    case :disk_log.open(opts) do
      {:ok, _log} -> :ok
      {:repaired, _log, _recovered, _bad_bytes} -> :ok
      error -> error
    end
  end

  defp append_to_manifest(name, edits) when is_list(edits) do
    edits = Enum.map(edits, &:erlang.term_to_binary/1)

    with :ok <- :disk_log.blog_terms(name, edits),
         :ok <- :disk_log.sync(name) do
      {:ok, Enum.reduce(edits, 0, &(&2 + byte_size(&1)))}
    end
  end

  defp append_to_manifest(name, edit), do: append_to_manifest(name, [edit])

  defp close_manifest(name) do
    :disk_log.close(name)
  end

  defp recover_rotated_manifest(file) do
    File.rename(rotated_file(file), file)
  end

  defp fetch_version(name) do
    case :disk_log.chunk(name, :start, 1) do
      {:error, _reason} = error ->
        error

      :eof ->
        version = new_version()
        append_to_manifest(name, {:snapshot, version})
        {:ok, version}

      {continuation, [{:snapshot, version}]} ->
        update_version(name, continuation, version)
    end
  end

  defp update_version(name, continuation, version) do
    case :disk_log.chunk(name, continuation, 1) do
      :eof ->
        {:ok, version}

      {continuation, [edit]} ->
        version = apply_edit(version, edit)
        update_version(name, continuation, version)
    end
  end

  defp apply_edit(version, []), do: version

  defp apply_edit(version, [edit | edits]) do
    version
    |> apply_edit(edit)
    |> apply_edit(edits)
  end

  defp apply_edit(version, {:sst_added, sst}) do
    ssts = MapSet.put(version.ssts, sst)
    %{version | ssts: ssts, count: version.count + 1}
  end

  defp apply_edit(version, {:sst_removed, sst}) do
    ssts = MapSet.delete(version.ssts, sst)
    %{version | ssts: ssts}
  end

  defp apply_edit(version, {:wal_added, wal}) do
    wal_rotations = MapSet.put(version.wal_rotations, wal)
    %{version | wal_rotations: wal_rotations}
  end

  defp apply_edit(version, {:wal_removed, wal}) do
    wal_rotations = MapSet.delete(version.wal_rotations, wal)
    %{version | wal_rotations: wal_rotations}
  end

  defp apply_edit(version, {:current_wal, wal}) do
    %{version | wal: wal}
  end

  defp apply_edit(version, {:seq, seq}) do
    %{version | seq: seq}
  end

  defp new_version,
    do: %{
      ssts: MapSet.new(),
      wal_rotations: MapSet.new(),
      wal: nil,
      count: 0,
      seq: 0
    }

  defp rotated_file(file), do: "#{file}.0"

  defp retry_export(%{waiting: {_, _, _, 0}}), do: {:error, :failed_to_export}

  defp retry_export(state) do
    %{waiting: {_, from, dir, retry}} = state
    start_export(state, dir, from, retry - 1)
  end

  defp start_export(state, dir, from, retry \\ @retry_attempts) do
    with {:ok, ref} <- export_snapshot(state, dir) do
      waiting = {ref, from, dir, retry}
      state = %{state | waiting: waiting}
      {:ok, state}
    end
  end

  defp export_snapshot(state, dir) do
    %{
      task_mod: task_mod,
      task_sup: task_sup,
      version: version,
      file: file
    } = state

    copied_manifest = Path.join(Path.dirname(file), "#{Path.basename(file)}.copy")

    with :ok <- File.cp(file, copied_manifest) do
      %{ref: ref} =
        task_mod.async(task_sup, fn ->
          filelist = tar_filelist(copied_manifest, version)
          export_tar(dir, copied_manifest, filelist)
        end)

      {:ok, ref}
    end
  end

  defp export_tar(dir, manifest, filelist) do
    tar_name = tar_name(dir)

    with :ok <- create_tar(tar_name, filelist) do
      {:ok, tar_name}
    else
      error ->
        rm_tar(tar_name)
        error
    end
  after
    File.rm!(manifest)
  end

  defp create_tar(name, filelist) do
    :erl_tar.create(name, filelist, [:compressed])
  end

  defp tar_filelist(manifest, version) do
    manifest_archive_name =
      manifest |> Path.basename() |> String.trim_trailing(".copy")

    manifest =
      {~c"#{manifest_archive_name}", ~c"#{manifest}"}

    rest =
      ([version.wal] ++
         MapSet.to_list(version.ssts) ++ MapSet.to_list(version.wal_rotations))
      |> Enum.map(fn file ->
        archive_name = Path.basename(file)
        {~c"#{archive_name}", ~c"#{file}"}
      end)

    [manifest | rest]
  end

  defp rm_tar(name) do
    File.rm(name)
  end

  defp tar_name(dir) do
    now = DateTime.utc_now(:second) |> DateTime.to_iso8601(:basic)
    filename = "goblin_#{now}.tar.gz"
    Path.join(dir, filename)
  end
end
