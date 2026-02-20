defmodule Goblin.ExportTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper

  setup_db()

  setup c do
    export_dir = Path.join(c.tmp_dir, "exports")
    File.mkdir!(export_dir)
    %{export_dir: export_dir}
  end

  describe "export/2" do
    @tag db_opts: [mem_limit: 2 * 1024]
    test "exports a .tar.gz snapshot from manifest", c do
      trigger_flush(c)

      assert_eventually do
        refute Goblin.flushing?(c.db)
      end

      assert {:ok, tar_name} = Goblin.Export.export(c.export_dir, c.manifest)
      assert File.exists?(tar_name)

      # verify we can extract the tar and it contains files
      {:ok, tar_content} = :erl_tar.extract(~c"#{tar_name}", [:memory, :compressed])
      assert length(tar_content) > 0

      # verify we can open a fresh database from the export
      unpack_dir = Path.join(c.tmp_dir, "unpack")
      File.mkdir!(unpack_dir)
      :ok = :erl_tar.extract(~c"#{tar_name}", [:compressed, cwd: ~c"#{unpack_dir}"])

      {_backup_db, _log} =
        ExUnit.CaptureLog.with_log(fn ->
          assert {:ok, backup_db} =
                   Goblin.start_link(name: Goblin.ExportBackup, data_dir: unpack_dir)

          backup_db
        end)
    end
  end
end
