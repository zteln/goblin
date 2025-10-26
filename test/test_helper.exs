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
        start_db(c.tmp_dir, Keyword.merge(unquote(opts), Map.get(c, :db_opts, [])))
      end
    end
  end

  def start_db(db_dir, opts \\ []) do
    id = opts[:id]

    db =
      ExUnit.Callbacks.start_link_supervised!(
        {Goblin, [db_dir: db_dir] ++ opts},
        id: id
      )

    [
      {Goblin.Writer, writer, _, _},
      {Goblin.Store, store, _, _},
      {Goblin.Compactor, compactor, _, _},
      {Goblin.WAL, wal, _, _},
      {Goblin.Manifest, manifest, _, _},
      {Goblin.RWLocks, rw_locks, _, _},
      {_, _, _, _}
    ] = Supervisor.which_children(db)

    %{
      db: db,
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

  def write_sst(dir, name, level_key, key_limit, data) do
    file_getter = fn -> Path.join(dir, "#{name}.goblin") end
    Goblin.SSTs.flush(data, level_key, key_limit, file_getter)
    file_getter.()
  end
end

ExUnit.start()
