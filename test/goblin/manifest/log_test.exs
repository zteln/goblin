defmodule Goblin.Manifest.LogTest do
  use ExUnit.Case, async: true

  alias Goblin.Manifest.Log

  @moduletag :tmp_dir

  defp log_name(ctx), do: :"#{ctx.test}"
  defp log_file(ctx), do: Path.join(ctx.tmp_dir, "manifest.goblin")

  describe "open_log/2" do
    test "opens a log successfully", ctx do
      assert {:ok, _log} = Log.open_log(log_name(ctx), log_file(ctx))
    end

    test "returns error for non-existent directory", ctx do
      assert {:error, _reason} =
               Log.open_log(log_name(ctx), Path.join(ctx.tmp_dir, "no/such/dir/manifest.goblin"))
    end

    test "handles reopen of existing log", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))
      :ok = Log.close_log(log)

      assert {:ok, _log} = Log.open_log(log_name(ctx), log_file(ctx))
    end
  end

  describe "close_log/1" do
    test "closes an open log", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))

      assert :ok = Log.close_log(log)
    end
  end

  describe "set_header/2" do
    test "sets the header on the log", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))

      assert :ok = Log.set_header(log, {:snapshot, %{test: true}})
    end
  end

  describe "append/3" do
    test "appends terms and updates header", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))

      assert :ok = Log.append(log, [{1, :sequence, 5}], {:snapshot, :header})

      terms = Log.stream_log!(log) |> Enum.to_list()
      assert [{1, :sequence, 5}] = terms
    end

    test "appends multiple terms in order", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))

      updates = [{1, :sequence, 5}, {2, :wal, "wal.goblin"}, {3, :disk_table_added, "sst.goblin"}]
      assert :ok = Log.append(log, updates, {:snapshot, :header})

      terms = Log.stream_log!(log) |> Enum.to_list()
      assert updates == terms
    end
  end

  describe "stream_log!/1" do
    test "streams empty log", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))

      assert [] == Log.stream_log!(log) |> Enum.to_list()
    end

    test "streams all terms across multiple appends in order", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))

      Log.append(log, [{1, :sequence, 1}], {:snapshot, :h1})
      Log.append(log, [{2, :wal, "w.goblin"}], {:snapshot, :h2})

      terms = Log.stream_log!(log) |> Enum.to_list()
      assert [{1, :sequence, 1}, {2, :wal, "w.goblin"}] = terms
    end

    test "terms survive close and reopen", ctx do
      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))
      Log.append(log, [{1, :sequence, 42}], {:snapshot, :header})
      Log.close_log(log)

      {:ok, log} = Log.open_log(log_name(ctx), log_file(ctx))
      terms = Log.stream_log!(log) |> Enum.to_list()

      assert [{1, :sequence, 42}] == terms
    end
  end
end
