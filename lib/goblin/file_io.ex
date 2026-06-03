defmodule Goblin.FileIO do
  @moduledoc false

  @magic "GOBLIN00"
  @header_size byte_size(<<@magic::binary, 0::integer-32, 0::integer-32, 0::integer-32>>)
  @default_block_size 512

  @default_modes [
    :raw,
    :read,
    :binary,
    read_ahead: 64 * 1024
  ]

  defstruct [
    :path,
    :iodev,
    :block_size
  ]

  @type t :: %__MODULE__{
          path: Path.t(),
          iodev: :file.io_device(),
          block_size: non_neg_integer()
        }

  @spec open!(Path.t(), keyword()) :: t()
  def open!(path, opts \\ []) do
    case open(path, opts) do
      {:ok, file} -> file
      _ -> raise "failed to open file #{inspect(path)}"
    end
  end

  @spec open(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(path, opts \\ []) do
    modes =
      case opts[:write?] do
        true -> [:append | @default_modes]
        _ -> @default_modes
      end

    with {:ok, iodev} <- :file.open(path, modes) do
      {:ok,
       %__MODULE__{
         path: path,
         iodev: iodev,
         block_size: opts[:block_size] || @default_block_size
       }}
    end
  end

  @spec append(t(), term(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(file, term, opts \\ []) do
    iolist = encode_to_iolist(term, file.block_size, opts[:compress?] || false)

    with :ok <- :file.write(file.iodev, iolist) do
      {:ok, :erlang.iolist_size(iolist)}
    end
  end

  @spec read(t()) :: {:ok, term()} | {:error, term()} | :eof
  def read(file) do
    with {:ok, header} <- :file.read(file.iodev, @header_size),
         {:ok, no_blocks, block_size, crc} <- decode_header(header),
         {:ok, payload} <- :file.read(file.iodev, no_blocks * block_size - @header_size),
         :ok <- validate_size(byte_size(payload), no_blocks * block_size - @header_size),
         :ok <- validate_crc(payload, crc) do
      {:ok, :erlang.binary_to_term(payload)}
    end
  end

  @spec pread(t(), non_neg_integer() | :eof) :: {:ok, term()} | {:error, term()} | :eof
  def pread(_file, pos) when pos < 0, do: {:error, :invalid_position}

  def pread(file, :eof) do
    case :filelib.file_size(file.path) do
      0 -> {:error, :empty}
      size -> pread(file, (div(size, file.block_size) - 1) * file.block_size)
    end
  end

  def pread(file, pos) do
    with {:ok, header} <- :file.pread(file.iodev, pos, @header_size),
         {:ok, no_blocks, block_size, crc} <- decode_header(header),
         {:ok, payload} <-
           :file.pread(file.iodev, pos + @header_size, no_blocks * block_size - @header_size),
         :ok <- validate_size(byte_size(payload), no_blocks * block_size - @header_size),
         :ok <- validate_crc(payload, crc) do
      {:ok, :erlang.binary_to_term(payload)}
    else
      {:error, :invalid_header} -> pread(file, (div(pos, file.block_size) - 1) * file.block_size)
      error -> error
    end
  end

  @spec stream(t()) ::
          Enumerable.t({:ok, any()} | {:corrupt, non_neg_integer()} | {:error, term()})
  def stream(file) do
    Stream.resource(
      fn -> {file, 0} end,
      fn
        :halt ->
          {:halt, nil}

        {file, pos} ->
          case read(file) do
            {:ok, terms} when is_list(terms) ->
              {:ok, pos} = :file.position(file.iodev, :cur)
              {[{:ok, terms}], {file, pos}}

            {:ok, term} ->
              {:ok, pos} = :file.position(file.iodev, :cur)
              {[{:ok, term}], {file, pos}}

            :eof ->
              {:halt, :eof}

            {:error, :invalid_size} ->
              {[{:corrupt, pos}], :halt}

            {:error, :invalid_header} ->
              {[{:corrupt, pos}], :halt}

            {:error, :invalid_crc} ->
              {[{:corrupt, pos}], :halt}

            {:error, _reason} = error ->
              {[error], :halt}
          end
      end,
      fn _ -> :ok end
    )
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(file), do: :file.close(file.iodev)

  @spec sync(t()) :: :ok | {:error, term()}
  def sync(file), do: :file.datasync(file.iodev)

  @spec truncate(t(), non_neg_integer()) :: :ok | {:error, term()}
  def truncate(file, pos) do
    with {:ok, _} <- :file.position(file.iodev, pos) do
      :file.truncate(file.iodev)
    end
  end

  @spec rename(Path.t(), Path.t()) :: :ok | {:error, term()}
  def rename(from, to), do: File.rename(from, to)

  @spec remove(Path.t()) :: :ok | {:error, term()}
  def remove(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      error -> error
    end
  end

  @spec size_of(Path.t()) :: non_neg_integer()
  def size_of(path), do: :filelib.file_size(path)

  defp encode_to_iolist(terms, block_size, compress?) do
    opts = if compress?, do: [:compressed], else: []
    payload = :erlang.term_to_iovec(terms, opts)
    payload_size = @header_size + :erlang.iolist_size(payload)
    no_blocks = div(payload_size + block_size - 1, block_size)
    padding = no_blocks * block_size - payload_size

    data = [
      payload,
      <<0::size(padding)-unit(8)>>
    ]

    [
      <<@magic::binary>>,
      <<no_blocks::integer-32>>,
      <<block_size::integer-32>>,
      <<:erlang.crc32(data)::integer-32>>
      | data
    ]
  end

  defp decode_header(<<
         @magic::binary,
         no_blocks::integer-32,
         block_size::integer-32,
         crc::integer-32
       >>),
       do: {:ok, no_blocks, block_size, crc}

  defp decode_header(_), do: {:error, :invalid_header}

  defp validate_size(size, size), do: :ok
  defp validate_size(_, _), do: {:error, :invalid_size}

  defp validate_crc(payload, crc) do
    case :erlang.crc32(payload) == crc do
      true -> :ok
      false -> {:error, :invalid_crc}
    end
  end
end
