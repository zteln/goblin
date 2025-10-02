defmodule TestHelper do
  require ExUnit.Assertions

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro setup_db(opts \\ []) do
    opts = Macro.expand(opts, __CALLER__)

    quote do
      setup c do
        start_db(c.tmp_dir, unquote(opts))
      end
    end
  end

  defmacro start_db(dir, opts \\ []) do
    id = opts[:id]
    name = opts[:name]
    level_limit = opts[:level_limit]
    sync_interval = opts[:sync_interval]
    wal_name = opts[:wal_name]

    quote do
      db =
        start_supervised!(
          {SeaGoat,
           name: unquote(name),
           dir: unquote(dir),
           level_limit: unquote(level_limit),
           wal_name: unquote(wal_name),
           sync_interval: unquote(sync_interval)},
          id: unquote(id)
        )

      [
        {SeaGoat.Writer, writer, _, _},
        {SeaGoat.Store, store, _, _},
        {SeaGoat.Compactor, compactor, _, _},
        {SeaGoat.WAL, wal, _, _},
        {SeaGoat.RWLocks, rw_locks, _, _}
      ] = Supervisor.which_children(db)

      %{
        db_id: unquote(id),
        writer: writer,
        store: store,
        compactor: compactor,
        wal: wal,
        rw_locks: rw_locks
      }
    end
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
