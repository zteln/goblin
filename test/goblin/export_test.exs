defmodule Goblin.ExportTest do
  use ExUnit.Case, async: true

  alias Goblin.Export

  @moduletag :tmp_dir

  defp create_file(dir, name, content \\ "data") do
    path = Path.join(dir, name)
    File.write!(path, content)
    path
  end

  describe "into_tar/2" do
    test "creates a tar.gz from a list of files", ctx do
      file_a = create_file(ctx.tmp_dir, "a.goblin")
      file_b = create_file(ctx.tmp_dir, "b.goblin")

      export_dir = Path.join(ctx.tmp_dir, "exports")
      File.mkdir!(export_dir)

      assert {:ok, tar_path} = Export.into_tar(export_dir, [file_a, file_b])
      assert File.exists?(tar_path)
      assert String.ends_with?(tar_path, ".tar.gz")

      {:ok, entries} = :erl_tar.extract(~c"#{tar_path}", [:memory, :compressed])
      names = Enum.map(entries, fn {name, _data} -> to_string(name) end) |> Enum.sort()

      assert names == ["a.goblin", "b.goblin"]
    end

    test "returns error for non-existent files", ctx do
      export_dir = Path.join(ctx.tmp_dir, "exports")
      File.mkdir!(export_dir)

      assert {:error, _reason} =
               Export.into_tar(export_dir, ["/no/such/file.goblin"])
    end

    test "handles empty file list", ctx do
      export_dir = Path.join(ctx.tmp_dir, "exports")
      File.mkdir!(export_dir)

      assert {:ok, tar_path} = Export.into_tar(export_dir, [])
      assert File.exists?(tar_path)

      {:ok, entries} = :erl_tar.extract(~c"#{tar_path}", [:memory, :compressed])
      assert entries == []
    end
  end
end
