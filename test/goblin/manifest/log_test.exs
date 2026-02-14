defmodule Goblin.Manifest.LogTest do
  use ExUnit.Case, async: true
  alias Goblin.Manifest.Log

  @moduletag :tmp_dir

  describe "append/2" do
    test "syncs to log", c do
      log_file = Path.join(c.tmp_dir, "log")
      {:ok, log} = Log.open(__MODULE__, log_file)
      %{size: size} = File.stat!(log_file)

      assert {:ok, append_size} = Log.append(log, :foo)

      %{size: new_size} = File.stat!(log_file)

      assert new_size - size >= append_size
    end
  end

  describe "stream_log!/1" do
    test "returns entire log", c do
      {:ok, log} = Log.open(__MODULE__, Path.join(c.tmp_dir, "log"))

      terms = [
        :foo,
        :bar,
        :baz
      ]

      Log.append(log, terms)

      assert terms == Log.stream_log!(log) |> Enum.to_list()
    end
  end
end
