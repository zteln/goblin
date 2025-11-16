defmodule TestHelper do
  @moduledoc false
  require ExUnit.Assertions
  require ExUnit.Callbacks
  require Goblin.ProcessRegistry

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro setup_db(opts \\ []) do
    quote do
      setup c do
        opts =
          Keyword.merge(
            unquote(opts),
            [name: __MODULE__] ++ Map.get(c, :db_opts, [])
          )

        start_db(c.tmp_dir, opts)
      end
    end
  end

  def start_db(db_dir, opts \\ []) do
    db =
      ExUnit.Callbacks.start_link_supervised!(
        {Goblin, [db_dir: db_dir] ++ opts},
        id: opts[:name]
      )

    [
      {_, proc_sup, _, _},
      {registry, _, _, _},
      {_, _, _, _}
    ] = Supervisor.which_children(db)

    [
      {Goblin.Writer, writer, _, _},
      {Goblin.Reader, reader, _, _},
      {Goblin.Store, store, _, _},
      {Goblin.Compactor, compactor, _, _},
      {Goblin.WAL, wal, _, _},
      {Goblin.Manifest, manifest, _, _},
      {_, _, _, _}
    ] = Supervisor.which_children(proc_sup)

    %{
      db: db,
      registry: registry,
      writer: writer,
      store: store,
      compactor: compactor,
      wal: wal,
      manifest: manifest,
      reader: reader
    }
  end

  def stop_db(id) do
    ExUnit.Callbacks.stop_supervised!(id)
  end

  def stream_flush_data(data, key_limit) do
    Stream.resource(
      fn -> data end,
      fn
        [] -> {:halt, :ok}
        [next | data] -> {[next], data}
      end,
      fn _ -> :ok end
    )
    |> Stream.chunk_every(key_limit)
  end

  def fake_sst(file, data) do
    {:ok, [sst]} =
      Goblin.SSTs.new([[data]], 0, file_getter: fn -> file end)

    sst
  end

  defmacro assert_eventually(opts \\ [], do: block) do
    timeout = opts[:timeout] || 1000
    step = opts[:step] || 100

    quote do
      assert_eventually(
        fn -> unquote(block) end,
        unquote(timeout),
        unquote(step)
      )
    end
  end

  def assert_eventually(_f, timeout, _step) when timeout <= 0,
    do: ExUnit.Assertions.assert(false, "Timed out")

  def assert_eventually(f, timeout, step) do
    try do
      f.()
    rescue
      ExUnit.AssertionError ->
        Process.sleep(step)
        assert_eventually(f, timeout - step, step)
    end
  end
end

ExUnit.start()
