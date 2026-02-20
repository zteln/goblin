defmodule Goblin.ManifestTest do
  use ExUnit.Case, async: true
  use Goblin.TestHelper
  use Mimic
  alias Goblin.Manifest
  import ExUnit.CaptureLog

  setup_db()

  test "durably tracks metadata across restarts", c do
    # update seq
    assert :ok == Manifest.update(c.manifest, seq: 999)
    assert %{seq: 999} = Manifest.snapshot(c.manifest, [:seq])

    # seq survives restart
    stop_db(name: __MODULE__)
    %{manifest: manifest} = start_db(name: __MODULE__, data_dir: c.tmp_dir)

    assert %{seq: 999} = Manifest.snapshot(manifest, [:seq])
  end

  test "failing to append to log stops the server", %{manifest: manifest} do
    Process.monitor(manifest)
    Process.flag(:trap_exit, true)

    Goblin.Manifest.Log
    |> expect(:append, fn _log, _terms ->
      {:error, :failed_to_append}
    end)

    Goblin.Manifest.Log
    |> allow(self(), manifest)

    with_log(fn ->
      assert {:error, :failed_to_append} = Manifest.update(manifest, seq: 1)
      assert_receive {:DOWN, _ref, :process, ^manifest, :failed_to_append}
    end)
  end

  test "recovers from interrupted rotation", c do
    # setup interrupted rotation
    Manifest.update(c.manifest, seq: 999)
    %{log_file: log_file} = :sys.get_state(c.manifest)
    File.cp!(log_file, "#{log_file}.rotation")

    # change state
    Manifest.update(c.manifest, seq: 1000)

    # restart database
    stop_db(name: __MODULE__)
    %{manifest: manifest} = start_db(data_dir: c.tmp_dir, name: __MODULE__)

    # recover previous state
    assert %{seq: 999} == Manifest.snapshot(manifest, [:seq])
  end
end
