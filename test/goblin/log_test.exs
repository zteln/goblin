defmodule Goblin.LogTest do
  use ExUnit.Case, async: true

  alias Goblin.Log

  @moduletag :tmp_dir

  defp log_name(c), do: :"#{c.test}"

  describe "open/3" do
    test "opens a log successfully", ctx do
      assert {:ok, _log} = Log.open(log_name(ctx), Path.join(ctx.tmp_dir, "log"))
    end

    test "returns error for non-existent directory", ctx do
      assert {:error, _reason} =
               Log.open(log_name(ctx), Path.join(ctx.tmp_dir, "no/such/dir/log"))
    end
  end

  describe "append/2" do
    test "appends a single term and syncs to disk", ctx do
      log_file = Path.join(ctx.tmp_dir, "log")
      {:ok, log} = Log.open(log_name(ctx), log_file)
      %{size: size_before} = File.stat!(log_file)

      assert {:ok, append_size} = Log.append(log, :hello)
      assert append_size > 0

      %{size: size_after} = File.stat!(log_file)
      assert size_after > size_before
    end

    test "appends a list of terms and returns cumulative size", ctx do
      {:ok, log} = Log.open(log_name(ctx), Path.join(ctx.tmp_dir, "log"))

      terms = [:foo, :bar, :baz]
      expected_size = Enum.reduce(terms, 0, &(&2 + :erlang.external_size(&1)))

      assert {:ok, ^expected_size} = Log.append(log, terms)
    end
  end

  describe "stream_log!/1" do
    test "streams back all appended terms in order", ctx do
      {:ok, log} = Log.open(log_name(ctx), Path.join(ctx.tmp_dir, "log"))

      terms = [:foo, {:bar, 1}, %{baz: [2, 3]}]
      Log.append(log, terms)

      assert terms == Log.stream_log!(log) |> Enum.to_list()
    end
  end

  describe "close/1" do
    test "closes an open log", ctx do
      {:ok, log} = Log.open(log_name(ctx), Path.join(ctx.tmp_dir, "log"))

      assert :ok = Log.close(log)
    end
  end
end
