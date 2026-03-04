defmodule Goblin.DiskTables.Handler do
  @moduledoc false

  defstruct [
    :file_handler,
    :file_offset
  ]

  @type t :: %__MODULE__{}

  @doc "Opens a new handler, raises if it fails."
  @spec open!(Path.t(), keyword()) :: t()
  def open!(file, opts \\ []) do
    case open(file, opts) do
      {:ok, handler} -> handler
      _ -> raise "failed to open file #{inspect(file)}"
    end
  end

  @doc "Opens a new handler, returning {:ok, handler} on success, {:error, reason} otherwise"
  @spec open(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(file, opts \\ []) do
    position = if opts[:start?], do: 0, else: :eof
    opts = [:binary, :read, :raw | List.wrap(opts[:write?] && :append)]

    with {:ok, file_handler} <- :file.open(file, opts),
         {:ok, file_offset} <- :file.position(file_handler, position) do
      {:ok, %__MODULE__{file_handler: file_handler, file_offset: file_offset}}
    end
  end

  @doc "Appends to the file opened via handler."
  @spec write(t(), binary()) :: {:ok, t()} | {:error, term()}
  def write(handler, bin) do
    with :ok <- :file.pwrite(handler.file_handler, handler.file_offset, bin) do
      {:ok, %{handler | file_offset: handler.file_offset + byte_size(bin)}}
    end
  end

  @doc "Reads from the handler from the end."
  @spec read_from_end(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_from_end(handler, position, size) do
    read(handler, handler.file_offset - position, size)
  end

  @doc "Reads `size` bytes from the handlers current offset."
  @spec read(t(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def read(handler, size) do
    read(handler, handler.file_offset, size)
  end

  @doc "Reads `size` bytes from provided position via handler."
  @spec read(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read(handler, position, size) do
    case :file.pread(handler.file_handler, position, size) do
      {:ok, bin} -> {:ok, bin}
      :eof -> {:error, :eof}
      error -> error
    end
  end

  @doc "Closes the handler."
  @spec close(t()) :: :ok | {:error, term()}
  def close(handler), do: :file.close(handler.file_handler)

  @doc "Syncs to disk."
  @spec sync(t()) :: :ok | {:error, term()}
  def sync(handler), do: :file.datasync(handler.file_handler)

  @doc "Increment handlers offset."
  @spec advance_offset(t(), non_neg_integer()) :: t()
  def advance_offset(handler, offset) do
    %{handler | file_offset: handler.file_offset + offset}
  end
end
