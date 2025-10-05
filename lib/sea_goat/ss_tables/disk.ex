defmodule SeaGoat.SSTables.Disk do
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
  Check `open/2` for options.
  """
  @spec open!(SeaGoat.db_file(), opts()) :: t()
  def open!(file, opts \\ []) do
    case open(file, opts) do
      {:ok, disk} -> disk
      _ -> raise "failed to open file #{inspect(file)}."
    end
  end

  @doc """
  Opens a file, defaulting to read-only mode and with the offset set to the end of the file.
  Returns `{:ok, %Disk{}}` if successful, `{:error, reason}` otherwise.
  Available options are:
  - `:write?`: a boolean indicating whether the file should be opened for writing (append only).
  - `:start?`: a boolean whether to set the offset on the start of the file or at the end.
  """
  @spec open(SeaGoat.db_file(), opts()) :: {:ok, t()} | {:error, term()}
  def open(file, opts \\ []) do
    open_opts = [:binary, :read, :raw] ++ if opts[:write?], do: [:append], else: []
    position = if opts[:start?], do: 0, else: :eof

    with {:ok, io} <- :file.open(file, open_opts),
         {:ok, offset} <- :file.position(io, position) do
      {:ok, %__MODULE__{io: io, offset: offset}}
    end
  end

  @doc """
  Writes `content` to the file at the position at `disk`'s offset.
  Returns `{:ok, disk}` if successful, `{:error, reason}` otherwise.
  """
  @spec write(t(), binary()) :: {:ok, t()} | {:error, term()}
  def write(disk, content) do
    with :ok <- :file.pwrite(disk.io, disk.offset, content) do
      {:ok, %{disk | offset: disk.offset + byte_size(content)}}
    end
  end

  @doc """
  Reads `size` bytes from the file from the with position `disk.offset - start`.
  """
  @spec read_from_end(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_from_end(disk, start, size), do: read(disk, disk.offset - start, size)

  @doc """
  Reads `size` bytes from the file with position `disk.offset`.
  """
  @spec read(t(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def read(disk, size), do: read(disk, disk.offset, size)

  @doc """
  Reads `size` bytes from the file from `position`.
  """
  @spec read(t(), non_neg_integer(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
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

  @doc """
  Adds `offset` to `disk`'s offset.
  """
  @spec advance_offset(t(), non_neg_integer()) :: t()
  def advance_offset(disk, offset), do: %{disk | offset: disk.offset + offset}

  @doc """
  Syncs the file.
  """
  @spec sync(t()) :: :ok | {:error, term()}
  def sync(disk) do
    :file.datasync(disk.io)
  end

  @doc """
  Closes the file.
  """
  @spec close(t()) :: :ok | {:error, term()}
  def close(disk) do
    :file.close(disk.io)
  end

  @doc """
  Renames `from` to `to`.
  """
  @spec rename(SeaGoat.db_file(), SeaGoat.db_file()) :: :ok | {:error, term()}
  def rename(from, to) do
    case :file.rename(from, to) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      e -> e
    end
  end

  @doc """
  Deletes `file`.
  """
  @spec rm(SeaGoat.db_file()) :: :ok | {:error, term()}
  def rm(file) do
    case :file.delete(file) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      e -> e
    end
  end
end
