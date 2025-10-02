defmodule SeaGoat.Store.FileManager do
  @file_suffix ".seagoat"
  @tmp_suffix ".tmp"
  @dump_suffix ".dump"

  def file_path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
  def tmp_file(file), do: file <> @tmp_suffix
  def dump_file(file), do: file <> @dump_suffix

  def valid_db_file?(file), do: String.ends_with?(file, [@file_suffix, @dump_suffix])

  def file_count_from_path(file) do
    [file_count] =
      file
      |> Path.basename()
      |> String.split([@file_suffix, @dump_suffix], trim: true)

    String.to_integer(file_count)
  end
end
