defmodule Goblin.Persistence do
  @moduledoc false

  alias Goblin.IOError

  @page_size 4096
  @header_size byte_size(<<0::integer-32, 0::integer-32>>)

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
      case {opts[:write?], opts[:new?]} do
        {true, true} -> [:append, :exclusive | @default_modes]
        {true, _} -> [:append | @default_modes]
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

  @spec offset_read(t(), non_neg_integer()) :: {:ok, term()} | {:error, term()}
  def offset_read(file, offset) do
    case :file.pread(file.iodev, offset, @page_size) do
      {:ok, <<header::binary-size(@header_size), rest::binary>>} ->
        read_record(
          {:ok, header},
          fn
            size when byte_size(rest) >= size -> {:ok, binary_part(rest, 0, size)}
            size -> :file.pread(file.iodev, offset + @header_size, size)
          end
        )

      {:ok, _short} ->
        {:error, :invalid_header}

      :eof ->
        {:error, :eof}

      error ->
        error
    end
  end

  @spec seq_read(t()) :: {:ok, term()} | {:error, term()}
  def seq_read(file) do
    read_record(
      :file.read(file.iodev, @header_size),
      fn size -> :file.read(file.iodev, size) end
    )
  end

  @spec read_footer(t()) :: {:ok, term()} | {:error, term()}
  def read_footer(file) do
    header_pos = :filelib.file_size(file.path) - @header_size

    read_record(
      :file.pread(file.iodev, header_pos, @header_size),
      fn size -> :file.pread(file.iodev, header_pos - size, size) end
    )
  end

  @spec stream(t()) ::
          Enumerable.t({:ok, any()} | {:corrupt, non_neg_integer()} | {:error, term()})
  def stream(file) do
    Stream.resource(
      fn ->
        case set_position(file, 0) do
          :ok -> {file, 0}
          _ -> :halt
        end
      end,
      fn
        :halt ->
          {:halt, nil}

        {file, pos} ->
          case seq_read(file) do
            {:ok, terms} when is_list(terms) ->
              {:ok, pos} = :file.position(file.iodev, :cur)
              {[{:ok, terms}], {file, pos}}

            {:ok, term} ->
              {:ok, pos} = :file.position(file.iodev, :cur)
              {[{:ok, term}], {file, pos}}

            {:error, :eof} ->
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

  @spec dirsync(Path.t()) :: :ok | {:error, term()}
  def dirsync(dir) do
    with {:ok, dir} <- :file.open(dir, [:read, :raw, :directory]) do
      try do
        :file.sync(dir)
      after
        :file.close(dir)
      end
    end
  end

  @spec truncate(t(), non_neg_integer()) :: :ok | {:error, term()}
  def truncate(file, pos) do
    with :ok <- set_position(file, pos) do
      :file.truncate(file.iodev)
    end
  end

  @spec set_position(t(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def set_position(file, pos) do
    with {:ok, _} <- :file.position(file.iodev, pos) do
      :ok
    end
  end

  @spec remove(Path.t()) :: :ok | {:error, term()}
  def remove(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      error -> error
    end
  end

  defp read_record(header_result, read_payload) do
    with {:ok, header} <- header_result,
         {:ok, size, crc} <- decode_header(header),
         {:ok, payload} <- read_payload(read_payload, size),
         :ok <- validate_size(byte_size(payload), size),
         :ok <- validate_crc(payload, crc) do
      decode_payload(payload)
    else
      :eof -> {:error, :eof}
      error -> error
    end
  end

  defp read_payload(read, size) do
    with :eof <- read.(size), do: {:error, :invalid_size}
  end

  defp encode_to_iolist(terms, compress?, footer?) do
    opts = if compress?, do: [:compressed], else: []
    payload = :erlang.term_to_iovec(terms, opts)
    payload_size = :erlang.iolist_size(payload)

    header = [
      <<payload_size::integer-32>>,
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
         payload_size::integer-32,
         crc::integer-32
       >>),
       do: {:ok, payload_size, crc}

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
