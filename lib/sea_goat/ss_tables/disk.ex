defmodule SeaGoat.SSTables.Disk do
  @moduledoc """
  Interface to on-disk files. 
  Relies on erlangs `:file` module for accessing files.
  All files are opened in `:raw` mode.
  """

  defstruct [
    :io,
    :offset
  ]

  @type io :: :file.io_device()
  @type offset :: non_neg_integer()
  @type opts :: keyword()
  @type t :: %__MODULE__{io: io(), offset: offset()}

  @doc """
  Opens a file and raises if it fails.
  """
  @spec open!(SeaGoat.Store.path(), opts()) :: t()
  def open!(path, opts \\ []) do
    case open(path, opts) do
      {:ok, disk} -> disk
      _ -> raise "failed to open file #{inspect(path)}."
    end
  end

  @spec open(SeaGoat.Store.path(), opts()) :: {:ok, t()} | {:error, term()}
  def open(path, opts \\ []) do
    open_opts = [:binary, :read, :raw] ++ if opts[:write?], do: [:append], else: []
    position = if opts[:start?], do: 0, else: :eof

    with {:ok, io} <- :file.open(path, open_opts),
         {:ok, offset} <- :file.position(io, position) do
      {:ok, %__MODULE__{io: io, offset: offset}}
    end
  end

  def write(disk, content) do
    with :ok <- :file.pwrite(disk.io, disk.offset, content) do
      {:ok, %{disk | offset: disk.offset + byte_size(content)}}
    end
  end

  def read_from_end(disk, start, size), do: read(disk, disk.offset - start, size)

  def read(disk, size), do: read(disk, disk.offset, size)

  def read(disk, position, size) do
    case :file.pread(disk.io, position, size) do
      {:ok, data} ->
        {:ok, data}

      :eof ->
        {:error, :eof}

      {:error, _reason} = e ->
        e
    end
  end

  def advance_offset(disk, offset), do: %{disk | offset: disk.offset + offset}

  def sync(disk) do
    :file.datasync(disk.io)
  end

  def close(disk) do
    :file.close(disk.io)
  end

  def rename(from, to) do
    case :file.rename(from, to) do
      {:error, :enoent} ->
        :ok

      :ok ->
        :ok

      e ->
        e
    end
  end

  def rm(path) do
    case :file.delete(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      e -> e
    end
  end
end
