defmodule TestHelper do
  @moduledoc false
  require ExUnit.Assertions
  require ExUnit.Callbacks
  require Goblin.Registry

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro setup_db(opts \\ []) do
    quote do
      @moduletag :tmp_dir

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
      _
    ] = Supervisor.which_children(db)

    [
      {Goblin.Broker, broker, _, _},
      {Goblin.MemTable, mem_table, _, _},
      {Goblin.DiskTables, disk_tables, _, _},
      {Goblin.Compactor, compactor, _, _},
      {Goblin.WAL, wal, _, _},
      {Goblin.Cleaner, cleaner, _, _},
      {Goblin.Manifest, manifest, _, _}
    ] = Supervisor.which_children(proc_sup)

    # Wait for mem_table to finish recovering...
    :sys.get_status(mem_table)

    %{
      db: db,
      registry: registry,
      broker: broker,
      mem_table: mem_table,
      disk_tables: disk_tables,
      compactor: compactor,
      wal: wal,
      cleaner: cleaner,
      manifest: manifest
    }
  end

  def stop_db(id) do
    ExUnit.Callbacks.stop_supervised!(id)
  end

  def trigger_flush(db, opts \\ []) do
    Stream.iterate(Keyword.get(opts, :key_start, 1), &(&1 + 1))
    |> Stream.map(&{&1, "#{Keyword.get(opts, :key_prefix, "v")}-#{&1}"})
    |> Stream.chunk_every(1000)
    |> Stream.transform(nil, fn chunk, acc ->
      if Goblin.is_flushing?(db) do
        {:halt, acc}
      else
        Goblin.put_multi(db, chunk)
        {chunk, acc}
      end
    end)
    |> Enum.to_list()
  end

  def generate_disk_table(data, opts \\ []) do
    next_file_f = fn ->
      file = Goblin.DiskTables.new_file(opts[:disk_tables_server])
      {"#{file}.tmp", file}
    end

    %{opts: disk_tables_server_opts} = :sys.get_state(opts[:disk_tables_server])

    opts =
      Keyword.merge(disk_tables_server_opts,
        level_key: opts[:level_key] || 0,
        compress?: false,
        max_sst_size: opts[:max_sst_size] || disk_tables_server_opts[:max_sst_size]
      )

    Goblin.DiskTables.DiskTable.write_new(data, next_file_f, opts)
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

Mimic.copy(Goblin.DiskTables)
Mimic.copy(Goblin.DiskTables.Handler)
Mimic.copy(Goblin.MemTable)
ExUnit.start()
