defmodule SeaGoat.Manifest do
  use GenServer

  @manifest_name :sea_goat_manifest
  @manifest_file "manifest.seagoat"
  @old_manifest_suffix ".old"
  @manifest_max_size 1024 * 1024

  defstruct [
    :log,
    :name,
    :file,
    :max_size,
    :version,
    size: 0
  ]

  def start_link(opts) do
    args = Keyword.take(opts, [:manifest_file, :dir])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def log_file_added(manifest, file) do
    GenServer.call(manifest, {:log_edit, {:file_added, file}})
  end

  def log_file_removed(manifest, file) do
    GenServer.call(manifest, {:log_edit, {:file_removed, file}})
  end

  def log_compaction(manifest, files, file) do
    GenServer.call(manifest, {:compaction, files, file})
  end

  def get_version(manifest) do
    GenServer.call(manifest, :get_version)
  end

  @impl GenServer
  def init(args) do
    name = args[:manifest_name] || @manifest_name
    file = Path.join(args[:dir], args[:manifest_file] || @manifest_file)
    max_size = args[:manifest_max_size] || @manifest_max_size

    if File.exists?(old_file(file)) do
      recover_old_manifest(file)
    end

    case open_manifest(name, file) do
      {:ok, log} ->
        {:ok, %__MODULE__{name: name, file: file, log: log, max_size: max_size},
         {:continue, :recover_version}}

      error ->
        {:stop, error}
    end
  end

  @impl GenServer
  def handle_call({:log_edit, edit}, _from, state) do
    case append_to_manifest(state.log, edit) do
      {:ok, size} ->
        version = apply_edit(state.version, edit)
        {:reply, :ok, %{state | size: state.size + size, version: version}, {:continue, :rotate}}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:compaction, files, file}, _from, state) do
    edits = [{:file_added, file} | Enum.map(files, &{:file_removed, &1})]

    case append_to_manifest(state.log, edits) do
      {:ok, size} ->
        version = apply_edits(state.version, edits)
        {:reply, :ok, %{state | size: state.size + size, version: version}, {:continue, :rotate}}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:get_version, _from, state) do
    reply = {MapSet.to_list(state.version.files), state.version.count}
    {:reply, reply, state}
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

  def handle_continue(:recover_version, state) do
    case fetch_version(state.log) do
      {:ok, version} ->
        {:noreply, %{state | version: version}}

      error ->
        {:stop, error, state}
    end
  end

  defp rotate(state) do
    with :ok <- close_manifest(state.log),
         :ok <- File.rename(state.file, old_file(state.file)),
         {:ok, log} <- open_manifest(state.name, state.file),
         {:ok, size} <- append_to_manifest(log, {:snapshot, state.version}),
         :ok <- File.rm(old_file(state.file)) do
      {:ok, %{state | log: log, size: size}}
    end
  end

  defp open_manifest(name, file) do
    opts = [name: name, file: ~c"#{file}"]

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      error -> error
    end
  end

  defp close_manifest(log) do
    :disk_log.close(log)
  end

  defp recover_old_manifest(file) do
    File.rename(old_file(file), file)
  end

  defp append_to_manifest(log, edits) when is_list(edits) do
    edits = Enum.map(edits, &:erlang.term_to_binary/1)

    with :ok <- :disk_log.blog_terms(log, edits),
         :ok <- :disk_log.sync(log) do
      {:ok, Enum.reduce(edits, 0, &(&2 + byte_size(&1)))}
    end
  end

  defp append_to_manifest(log, edit), do: append_to_manifest(log, [edit])

  defp fetch_version(log) do
    case :disk_log.chunk(log, :start, 1) do
      :eof ->
        # New log, append new version
        version = %{files: MapSet.new(), count: 0}
        append_to_manifest(log, {:snapshot, version})
        {:ok, version}

      {continuation, [{:snapshot, version}]} ->
        update_version(log, continuation, version)
    end
  end

  defp update_version(log, continuation, version) do
    case :disk_log.chunk(log, continuation, 1) do
      :eof ->
        {:ok, version}

      {continuation, [edit]} ->
        version = apply_edit(version, edit)
        update_version(log, continuation, version)
    end
  end

  defp apply_edits(version, []), do: version

  defp apply_edits(version, [edit | edits]) do
    version
    |> apply_edit(edit)
    |> apply_edits(edits)
  end

  defp apply_edit(version, {:file_added, file}) do
    files = MapSet.put(version.files, file)
    %{version | files: files, count: version.count + 1}
  end

  defp apply_edit(version, {:file_removed, file}) do
    files = MapSet.delete(version.files, file)
    %{version | files: files}
  end

  defp old_file(file), do: file <> @old_manifest_suffix
end
