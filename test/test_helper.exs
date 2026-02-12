defmodule Goblin.TestHelper do
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

        if Map.get(c, :start_db?, true),
          do: start_db([data_dir: c.tmp_dir] ++ opts),
          else: :ok
      end
    end
  end

  def start_db(opts \\ []) do
    db =
      ExUnit.Callbacks.start_link_supervised!(
        {Goblin, opts},
        id: opts[:name]
      )

    [
      {_, proc_sup, _, _},
      {registry, _, _, _},
      _
    ] = Supervisor.which_children(db)

    [
      {Goblin.MemTables, mem_tables, _, _},
      {Goblin.DiskTables, disk_tables, _, _},
      {Goblin.Broker, broker, _, _},
      {Goblin.Manifest, manifest, _, _}
    ] = Supervisor.which_children(proc_sup)

    %{
      db: db,
      registry: registry,
      broker: broker,
      mem_tables: mem_tables,
      disk_tables: disk_tables,
      manifest: manifest
    }
  end

  def stop_db(opts \\ []) do
    ExUnit.Callbacks.stop_supervised!(opts[:name])
  end

  def trigger_flush(db, dir) do
    dir_count = File.ls!(dir) |> length()

    StreamData.term()
    |> Stream.chunk_every(50)
    |> Stream.transform(nil, fn keys, acc ->
      values = StreamData.term() |> Enum.take(length(keys))
      pairs = Enum.zip(keys, values)

      if length(File.ls!(dir)) > dir_count do
        {:halt, acc}
      else
        Goblin.put_multi(db, pairs)
        {pairs, acc}
      end
    end)
    |> Enum.to_list()
  end

  def uniq_by_value(list, f \\ & &1) do
    Enum.reduce(list, [], fn item, acc ->
      case Enum.any?(acc, &(f.(&1) == f.(item))) do
        true -> acc
        false -> [item | acc]
      end
    end)
    |> Enum.reverse()
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

  def assert_eventually(f, timeout, _step) when timeout <= 0,
    do: f.()

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
Mimic.copy(Goblin.DiskTables.DiskTable)
Mimic.copy(Goblin.DiskTables.Handler)
Mimic.copy(Goblin.DiskTables.Legacy.Encoder)
Mimic.copy(Goblin.Manifest.Log)
ExUnit.start()
