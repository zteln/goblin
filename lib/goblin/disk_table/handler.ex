defmodule Goblin.DiskTable.Handler do
  @moduledoc false

  defstruct [
    :file_handler,
    :file_offset
  ]

  @type t :: %__MODULE__{}

  @default_open_opts [:binary, :read, :raw, read_ahead: 64 * 1024]

  @spec open!(Path.t(), keyword()) :: t()
  def open!(file, opts \\ []) do
    case open(file, opts) do
      {:ok, handler} -> handler
      _ -> raise "failed to open file #{inspect(file)}"
    end
  end

  @spec open(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(file, opts \\ []) do
    position = if opts[:start?], do: 0, else: :eof

    opts =
      case opts[:write?] do
        true -> [:append | @default_open_opts]
        _ -> @default_open_opts
      end

    with {:ok, file_handler} <- :file.open(file, opts),
         {:ok, file_offset} <- :file.position(file_handler, position) do
      {:ok, %__MODULE__{file_handler: file_handler, file_offset: file_offset}}
    end
  end

  @spec write(t(), binary()) :: {:ok, t()} | {:error, term()}
  def write(handler, bin) do
    with :ok <- :file.pwrite(handler.file_handler, handler.file_offset, bin) do
      {:ok, %{handler | file_offset: handler.file_offset + byte_size(bin)}}
    end
  end

  @spec read_from_end(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_from_end(handler, position, size) do
    read(handler, handler.file_offset - position, size)
  end

  @spec read(t(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def read(handler, size) do
    read(handler, handler.file_offset, size)
  end

  @spec read(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read(handler, position, size) do
    case :file.pread(handler.file_handler, position, size) do
      {:ok, bin} -> {:ok, bin}
      :eof -> {:error, :eof}
      error -> error
    end
  end

  @spec seq_read(t(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def seq_read(handler, size) do
    case :file.read(handler.file_handler, size) do
      {:ok, bin} -> {:ok, bin}
      :eof -> {:error, :eof}
      error -> error
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(handler), do: :file.close(handler.file_handler)

  @spec sync(t()) :: :ok | {:error, term()}
  def sync(handler), do: :file.datasync(handler.file_handler)
end
