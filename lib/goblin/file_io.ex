defmodule Goblin.FileIO do
  @moduledoc false

  alias Goblin.IOError

  @magic "GOBLIN00"
  @header_size byte_size(<<@magic::binary, 0::integer-64, 0::integer-32>>)

  @default_modes [
    :raw,
    :read,
    :binary,
    read_ahead: 64 * 1024
  ]

  defstruct [
    :path,
    :iodev
  ]

  @type t :: %__MODULE__{
          path: Path.t(),
          iodev: :file.io_device()
        }

  @spec open!(Path.t(), keyword()) :: t()
  def open!(path, opts \\ []) do
    case open(path, opts) do
      {:ok, file} -> file
      {:error, reason} -> raise IOError, operation: :open, path: path, reason: reason
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
         iodev: iodev
       }}
    end
  end

  @spec append(t(), term(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(file, term, opts \\ []) do
    compress? = opts[:compress?] || false
    footer? = opts[:footer?] || false
    iolist = encode_to_iolist(term, compress?, footer?)

    with :ok <- :file.write(file.iodev, iolist) do
      {:ok, :erlang.iolist_size(iolist)}
    end
  end

  @spec read(t(), keyword()) :: {:ok, term()} | {:error, term()} | :eof
  def read(file, opts \\ []) do
    reader =
      cond do
        opts[:offset] ->
          fn offset, size -> :file.pread(file.iodev, opts[:offset] + offset, size) end

        true ->
          fn _, size -> :file.read(file.iodev, size) end
      end

    with {:ok, header} <- reader.(0, @header_size),
         {:ok, size, crc} <- decode_header(header),
         {:ok, payload} <- reader.(@header_size, size),
         :ok <- validate_size(byte_size(payload), size),
         :ok <- validate_crc(payload, crc, Keyword.get(opts, :verify_crc?, true)) do
      decode_payload(payload)
    end
  end

  @spec read_footer(t()) :: {:ok, term()} | {:error, term()} | :eof
  def read_footer(file) do
    size = :filelib.file_size(file.path)

    with {:ok, header} <- :file.pread(file.iodev, size - @header_size, @header_size),
         {:ok, payload_size, crc} <- decode_header(header),
         {:ok, payload} <-
           :file.pread(file.iodev, size - (@header_size + payload_size), payload_size),
         :ok <- validate_size(byte_size(payload), payload_size),
         :ok <- validate_crc(payload, crc, true) do
      decode_payload(payload)
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
    with {:ok, _} <- set_position(file, pos) do
      :file.truncate(file.iodev)
    end
  end

  @spec set_position(t(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def set_position(file, pos), do: :file.position(file.iodev, pos)

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

  defp encode_to_iolist(terms, compress?, footer?) do
    opts = if compress?, do: [:compressed], else: []
    payload = :erlang.term_to_iovec(terms, opts)
    payload_size = :erlang.iolist_size(payload)

    header = [
      <<@magic::binary>>,
      <<payload_size::integer-64>>,
      <<:erlang.crc32(payload)::integer-32>>
    ]

    case footer? do
      true -> [header, payload, header]
      _ -> [header, payload]
    end
  end

  defp decode_payload(payload) do
    {:ok, :erlang.binary_to_term(payload)}
  rescue
    ArgumentError -> {:error, :invalid_term}
  end

  defp decode_header(<<
         @magic::binary,
         payload_size::integer-64,
         crc::integer-32
       >>),
       do: {:ok, payload_size, crc}

  defp decode_header(_), do: {:error, :invalid_header}

  defp validate_size(size, size), do: :ok
  defp validate_size(_, _), do: {:error, :invalid_size}

  defp validate_crc(_payload, _crc, false), do: :ok

  defp validate_crc(payload, crc, _) do
    case :erlang.crc32(payload) == crc do
      true -> :ok
      false -> {:error, :invalid_crc}
    end
  end
end
