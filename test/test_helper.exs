defmodule Goblin.TestHelper do
  @moduledoc false
  require ExUnit.Assertions
  require ExUnit.Callbacks
  require Goblin.Registry

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__),
        only: [
          setup_db: 0,
          setup_db: 1,
          start_db: 0,
          start_db: 1,
          stop_db: 0,
          stop_db: 1,
          trigger_flush: 1,
          trigger_flush: 2,
          assert_eventually: 1,
          assert_eventually: 3
        ]
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

    _new_ref = Goblin.Broker.SnapshotRegistry.new_ref(Module.concat(opts[:name], Broker))

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

  def trigger_flush(opts, key_generator \\ &StreamData.term/0) do
    %{wal: wal} = Goblin.Manifest.snapshot(opts[:manifest], [:wal])

    key_generator.()
    |> Stream.chunk_every(50)
    |> Stream.transform(nil, fn keys, acc ->
      values = StreamData.term() |> Enum.take(length(keys))
      pairs = Enum.zip(keys, values)
      %{wal: new_wal} = Goblin.Manifest.snapshot(opts[:manifest], [:wal])

      if new_wal != wal do
        {:halt, acc}
      else
        Goblin.put_multi(opts[:db], pairs)
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
Mimic.copy(Goblin.MemTables)
Mimic.copy(Goblin.MemTables.WAL)
ExUnit.start()
