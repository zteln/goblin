defmodule Goblin.ExportTest do
  use ExUnit.Case, async: true
  use TestHelper

  setup_db()

  setup c do
    export_dir = Path.join(c.tmp_dir, "exports")
    File.mkdir!(export_dir)
    %{export_dir: export_dir}
  end

  describe "export/2" do
    @tag db_opts: [mem_limit: 2 * 1024]
    test "exports a .tar.gz snapshot from manifest", c do
      trigger_flush(c.db)

      assert_eventually do
        refute Goblin.is_flushing?(c.db)
      end

      assert {:ok, tar_name} = Goblin.Export.export(c.export_dir, c.manifest)
      assert File.exists?(tar_name)

      {:ok, tar_content} = :erl_tar.extract(~c"#{tar_name}", [:memory, :compressed])

      Enum.each(tar_content, fn {name, content} ->
        filename = Path.join(c.tmp_dir, to_string(name))
        assert content == File.read!(filename)
      end)
    end
  end
end
