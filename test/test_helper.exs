defmodule TestHelper do
  @moduledoc false
  require ExUnit.Assertions
  require ExUnit.Callbacks

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro setup_db(opts \\ []) do
    quote do
      setup c do
        start_db(c.tmp_dir, unquote(opts))
      end
    end
  end

  def start_db(db_dir, opts \\ []) do
    id = opts[:id]

    db =
      ExUnit.Callbacks.start_link_supervised!(
        {SeaGoat, [db_dir: db_dir] ++ opts},
        id: id
      )

    [
      {SeaGoat.Writer, writer, _, _},
      {SeaGoat.Store, store, _, _},
      {SeaGoat.Compactor, compactor, _, _},
      {SeaGoat.WAL, wal, _, _},
      {SeaGoat.Manifest, manifest, _, _},
      {SeaGoat.RWLocks, rw_locks, _, _}
    ] = Supervisor.which_children(db)

    %{
      db_id: id,
      writer: writer,
      store: store,
      compactor: compactor,
      wal: wal,
      manifest: manifest,
      rw_locks: rw_locks
    }
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
