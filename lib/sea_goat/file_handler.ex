defmodule SeaGoat.FileHandler do
  def open(path, opts \\ []) do
    open_opts = [:binary, :read, :raw | List.wrap(if opts[:write?], do: :append)]
    position = if opts[:start?], do: 0, else: :eof

    with {:ok, io} <- :file.open(path, open_opts),
         {:ok, offset} <- :file.position(io, position) do
      {:ok, io, offset}
    end
  end

  def write(io, offset, content) do
    with :ok <- :file.pwrite(io, offset, content) do
      {:ok, offset + byte_size(content)}
    end
  end

  def read(io, offset, size) do
    :file.pread(io, offset, size)
  end

  def rename(from, to) do
    :file.rename(from, to)
  end

  def sync(io) do
    :file.datasync(io)
  end

  def close(io) do
    :file.close(io)
  end

  def rm(path) do
    :file.delete(path)
  end

  def path(dir, name), do: Path.join(dir, "#{name}.seagoat")
end
