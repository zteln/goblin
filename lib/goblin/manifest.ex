defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.Persistence

  @alog_file "manifest.a"
  @blog_file "manifest.b"

  defstruct [
    :a,
    :b,
    :data_dir,
    :active,
    version: 0,
    snapshot: []
  ]

  @type snapshot :: list(Path.t())

  @type t :: %__MODULE__{
          a: Persistence.t(),
          b: Persistence.t(),
          data_dir: Path.t(),
          active: :a | :b,
          version: non_neg_integer(),
          snapshot: snapshot()
        }

  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
  def open(data_dir) do
    alog_path = apath(data_dir)
    blog_path = bpath(data_dir)

    with :ok <- ensure_unique_access(alog_path),
         :ok <- ensure_unique_access(blog_path),
         {:ok, alog} <- Persistence.open(alog_path, write?: true),
         {:ok, blog} <- Persistence.open(blog_path, write?: true),
         :ok <- Persistence.dirsync(data_dir) do
      manifest = %__MODULE__{
        data_dir: data_dir,
        a: alog,
        b: blog
      }

      recover_manifest(manifest)
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(manifest) do
    with :ok <- Persistence.close(manifest.a) do
      Persistence.close(manifest.b)
    end
  end

  @spec snapshot(t()) :: snapshot()
  def snapshot(manifest) do
    [@alog_file, @blog_file | manifest.snapshot]
    |> Enum.map(&Path.join(manifest.data_dir, &1))
  end

  @spec update(t(), list(Path.t()), list(Path.t())) :: {:ok, t()} | {:error, term()}
  def update(manifest, add, del) do
    add = Enum.map(add, &Path.basename/1)
    del = Enum.map(del, &Path.basename/1)

    snapshot =
      (manifest.snapshot ++ add)
      |> Enum.reject(&(&1 in del))

    manifest = %{manifest | snapshot: snapshot}

    with :ok <- Persistence.dirsync(manifest.data_dir) do
      write_snapshot(manifest)
    end
  end

  defp write_snapshot(manifest) do
    log_key = switch(manifest.active)
    log = Map.get(manifest, log_key)
    version = manifest.version + 1

    with :ok <- Persistence.truncate(log, 0),
         {:ok, _} <- Persistence.append(log, {version, manifest.snapshot}),
         :ok <- Persistence.sync(log) do
      {:ok, %{manifest | active: log_key, version: version}}
    end
  end

  defp recover_manifest(manifest) do
    a = recover_snapshot(manifest.a)
    b = recover_snapshot(manifest.b)

    case {a, b} do
      {{:ok, a_ver, a_snapshot}, {:ok, b_ver, _}} when a_ver >= b_ver ->
        {:ok, %{manifest | active: :a, version: a_ver, snapshot: a_snapshot}}

      {{:ok, _, _}, {:ok, b_ver, b_snapshot}} ->
        {:ok, %{manifest | active: :b, version: b_ver, snapshot: b_snapshot}}

      {{:ok, a_ver, a_snapshot}, :corrupt} ->
        {:ok, %{manifest | active: :a, version: a_ver, snapshot: a_snapshot}}

      {:corrupt, {:ok, b_ver, b_snapshot}} ->
        {:ok, %{manifest | active: :b, version: b_ver, snapshot: b_snapshot}}

      {:corrupt, :corrupt} ->
        {:error, :corrupt_manifest}

      {{:error, _reason} = e, _} ->
        e

      {_, {:error, _reason} = e} ->
        e
    end
  end

  defp recover_snapshot(log) do
    case Persistence.offset_read(log, 0) do
      {:ok, {version, snapshot}} ->
        {:ok, version, snapshot}

      {:error, :eof} ->
        {:ok, 0, []}

      {:error, reason}
      when reason in [
             :invalid_crc,
             :invalid_size,
             :invalid_header,
             :invalid_term
           ] ->
        :corrupt

      error ->
        error
    end
  end

  defp ensure_unique_access(path) do
    case :global.set_lock({{__MODULE__, path}, self()}, [node()], 0) do
      true -> :ok
      false -> {:error, :manifest_in_use_already}
    end
  end

  defp apath(dir), do: Path.join(dir, @alog_file)
  defp bpath(dir), do: Path.join(dir, @blog_file)
  defp switch(:a), do: :b
  defp switch(:b), do: :a
end
