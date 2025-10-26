defmodule Goblin.SSTs.Disk do
  @moduledoc false
  defstruct [
    :io,
    :offset
  ]

  @type io :: :file.io_device()
  @type offset :: non_neg_integer()
  @type opts :: [write?: boolean(), start?: boolean()]
  @type t :: %__MODULE__{io: io(), offset: offset()}

  @spec open!(Goblin.db_file(), opts()) :: t()
  def open!(file, opts \\ []) do
    case open(file, opts) do
      {:ok, disk} -> disk
      _ -> raise "failed to open file #{inspect(file)}."
    end
  end

  @spec open(Goblin.db_file(), opts()) :: {:ok, t()} | {:error, term()}
  def open(file, opts \\ []) do
    open_opts = [:binary, :read, :raw] ++ if opts[:write?], do: [:append], else: []
    position = if opts[:start?], do: 0, else: :eof

    with {:ok, io} <- :file.open(file, open_opts),
         {:ok, offset} <- :file.position(io, position) do
      {:ok, %__MODULE__{io: io, offset: offset}}
    end
  end

  @spec write(t(), binary()) :: {:ok, t()} | {:error, term()}
  def write(disk, content) do
    with :ok <- :file.pwrite(disk.io, disk.offset, content) do
      {:ok, %{disk | offset: disk.offset + byte_size(content)}}
    end
  end

  @spec read_from_end(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_from_end(disk, start, size), do: read(disk, disk.offset - start, size)

  @spec read(t(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def read(disk, size), do: read(disk, disk.offset, size)

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

  @spec advance_offset(t(), non_neg_integer()) :: t()
  def advance_offset(disk, offset), do: %{disk | offset: disk.offset + offset}

  def start_of_file(disk), do: %{disk | offset: 0}

  @spec sync(t()) :: :ok | {:error, term()}
  def sync(disk) do
    :file.datasync(disk.io)
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(disk) do
    :file.close(disk.io)
  end

  @spec rename(Goblin.db_file(), Goblin.db_file()) :: :ok | {:error, term()}
  def rename(from, to) do
    case :file.rename(from, to) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      e -> e
    end
  end

  @spec rm(Goblin.db_file()) :: :ok | {:error, term()}
  def rm(file) do
    case :file.delete(file) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      e -> e
    end
  end
end
