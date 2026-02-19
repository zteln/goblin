defmodule Goblin.DiskTables do
  @moduledoc false
  use GenServer
  require Logger

  import Goblin.Registry, only: [via: 2]

  alias Goblin.{
    Manifest,
    Broker
  }

  alias Goblin.DiskTables.{
    DiskTable,
    StreamIterator,
    Legacy
  }

  @flush_level 0
  @file_suffix ".goblin"

  defstruct [
    :levels,
    :data_dir,
    :broker,
    :manifest,
    :registry,
    :compacting,
    :opts,
    file_number: 0
  ]

  @type state :: %__MODULE__{}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name] || __MODULE__

    args = [
      name: name,
      data_dir: opts[:data_dir],
      manifest: opts[:manifest],
      broker: opts[:broker],
      registry: opts[:registry],
      bf_fpp: opts[:bf_fpp],
      bf_bit_array_size: opts[:bf_bit_array_size],
      max_sst_size: opts[:max_sst_size],
      flush_level_file_limit: opts[:flush_level_file_limit],
      level_base_size: opts[:level_base_size],
      level_size_multiplier: opts[:level_size_multiplier]
    ]

    GenServer.start_link(__MODULE__, args, name: via(opts[:registry], name))
  end

  @doc "Create new disk tables with data from `stream`. Automatically adds them for compaction and registers them in the Broker table registry."
  @spec new(GenServer.server(), Enumerable.t(Goblin.triple()), keyword()) ::
          {:ok, [DiskTable.t()]} | {:error, term()}
  def new(server, stream, opts) do
    next_file_f = fn ->
      file = GenServer.call(server, :new_file)
      tmp_file = tmp_file(file)

      if File.exists?(tmp_file),
        do: File.rm!(tmp_file)

      {tmp_file, file}
    end

    opts = Keyword.merge(opts, GenServer.call(server, :get_opts))

    with {:ok, disk_tables} <- DiskTable.write_new(stream, next_file_f, opts) do
      GenServer.call(server, {:add_new, disk_tables})
      {:ok, disk_tables}
    end
  end

  @doc "Returns a boolean indicating whether there is an ongoing compaction or not."
  @spec compacting?(GenServer.server()) :: boolean()
  def compacting?(server) do
    GenServer.call(server, :compacting?)
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       data_dir: args[:data_dir],
       levels: %{},
       broker: args[:broker],
       manifest: via(args[:registry], args[:manifest]),
       registry: args[:registry],
       opts: [
         bf_fpp: args[:bf_fpp],
         bf_bit_array_size: args[:bf_bit_array_size],
         max_sst_size: args[:max_sst_size],
         flush_level_file_limit: args[:flush_level_file_limit],
         level_base_size: args[:level_base_size],
         level_size_multiplier: args[:level_size_multiplier]
       ]
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:add_new, disk_tables}, _from, state) do
    levels =
      Enum.reduce(disk_tables, state.levels, fn disk_table, acc ->
        Broker.register_table(
          state.broker,
          disk_table.file,
          disk_table.level_key,
          disk_table,
          &File.rm(&1.file)
        )

        Map.update(acc, disk_table.level_key, [disk_table], &[disk_table | &1])
      end)

    {:reply, :ok, %{state | levels: levels}, {:continue, :maybe_compact}}
  end

  def handle_call(:new_file, _from, state) do
    new_file = file_path(state.data_dir, state.file_number)
    {:reply, new_file, %{state | file_number: state.file_number + 1}}
  end

  def handle_call(:get_opts, _from, state) do
    {:reply, state.opts, state}
  end

  def handle_call(:compacting?, _from, %{compacting: nil} = state),
    do: {:reply, false, state}

  def handle_call(:compacting?, _from, state),
    do: {:reply, true, state}

  @impl GenServer
  def handle_continue(:maybe_compact, %{compacting: nil} = state) do
    level_keys =
      state.levels
      |> Map.keys()
      |> Enum.sort()

    {:noreply, maybe_compact(state, level_keys)}
  end

  def handle_continue(:maybe_compact, state), do: {:noreply, state}

  def handle_continue(:recover_state, state) do
    %{disk_tables: disk_table_names, count: count} =
      Manifest.snapshot(state.manifest, [:disk_tables, :count])

    case recover_disk_tables(disk_table_names, state.opts) do
      {:ok, disk_tables} ->
        levels =
          Enum.reduce(disk_tables, state.levels, fn disk_table, acc ->
            Broker.register_table(
              state.broker,
              disk_table.file,
              disk_table.level_key,
              disk_table,
              &File.rm(&1.file)
            )

            Map.update(acc, disk_table.level_key, [disk_table], &[disk_table | &1])
          end)

        Broker.inc_ready(state.broker)

        state = %{state | levels: levels, file_number: count}
        {:noreply, state, {:continue, :maybe_compact}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl GenServer
  def handle_info({ref, {:ok, :compacted}}, %{compacting: ref} = state) do
    {:noreply, %{state | compacting: nil}, {:continue, :maybe_compact}}
  end

  def handle_info({ref, {:error, reason}}, %{compacting: ref} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{compacting: ref} = state) do
    {:stop, reason, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    Broker.deinc_ready(state.broker)
    :ok
  end

  defp maybe_compact(state, []), do: state

  defp maybe_compact(state, [level_key | level_keys]) do
    level = Map.get(state.levels, level_key, [])

    if exceeding_level_limit?(level, level_key, state.opts) do
      compact(state, level_key)
    else
      maybe_compact(state, level_keys)
    end
  end

  defp compact(state, source_level_key) do
    me = self()

    %{
      manifest: manifest,
      broker: broker,
      registry: registry,
      levels: levels
    } = state

    target_level_key = source_level_key + 1

    {sources, targets} =
      if source_level_key == @flush_level do
        sources = Map.get(levels, source_level_key, [])

        min_source_key =
          sources
          |> Enum.map(&elem(&1.key_range, 0))
          |> Enum.min()

        max_source_key =
          sources
          |> Enum.map(&elem(&1.key_range, 1))
          |> Enum.max()

        targets =
          levels
          |> Map.get(target_level_key, [])
          |> find_overlapping({min_source_key, max_source_key})

        {sources, targets}
      else
        source =
          levels
          |> Map.get(source_level_key, [])
          |> Enum.min_by(&elem(&1.seq_range, 0))

        targets =
          levels
          |> Map.get(target_level_key, [])
          |> find_overlapping(source.key_range)

        {[source], targets}
      end

    filter_tombstones = target_level_key >= Enum.max(Map.keys(levels), fn -> 0 end)

    source_ids = Enum.map(sources, & &1.file)
    target_ids = Enum.map(targets, & &1.file)

    %{ref: ref} =
      Task.async(fn ->
        opts = [
          level_key: target_level_key,
          compress?: target_level_key > 1
        ]

        iterators = Enum.map(sources ++ targets, &StreamIterator.new/1)

        stream =
          Goblin.Iterator.k_merge_stream(
            fn -> iterators end,
            filter_tombstones: filter_tombstones
          )

        with {:ok, new_disk_tables} <- new(me, stream, opts),
             :ok <-
               Manifest.update(manifest,
                 remove_disk_tables: source_ids ++ target_ids,
                 add_disk_tables: Enum.map(new_disk_tables, & &1.file)
               ) do
          Enum.each(sources ++ targets, fn disk_table ->
            Broker.soft_delete(broker, via(registry, broker), disk_table.file)
          end)

          {:ok, :compacted}
        end
      end)

    levels =
      levels
      |> Map.update(source_level_key, [], fn level ->
        Enum.reject(level, &(&1.file in source_ids))
      end)
      |> Map.update(target_level_key, [], fn level ->
        Enum.reject(level, &(&1.file in target_ids))
      end)

    %{state | compacting: ref, levels: levels}
  end

  defp find_overlapping(targets, key_range, acc \\ [])
  defp find_overlapping([], _key_range, acc), do: acc

  defp find_overlapping([target | targets], {min, max}, acc) do
    %{key_range: {target_min, target_max}} = target

    cond do
      target_max < min -> find_overlapping(targets, {min, max}, acc)
      target_min > max -> find_overlapping(targets, {min, max}, acc)
      true -> find_overlapping(targets, {min, max}, [target | acc])
    end
  end

  defp exceeding_level_limit?(level, @flush_level, opts) do
    Enum.count(level) >= opts[:flush_level_file_limit]
  end

  defp exceeding_level_limit?(level, level_key, opts) do
    Enum.sum_by(level, & &1.size) >=
      opts[:level_base_size] * opts[:level_size_multiplier] ** (level_key - 1)
  end

  defp recover_disk_tables(disk_table_names, opts, acc \\ [])
  defp recover_disk_tables([], _opts, acc), do: {:ok, acc}

  defp recover_disk_tables([disk_table_name | disk_table_names], opts, acc) do
    case DiskTable.parse(disk_table_name) do
      {:ok, disk_table} ->
        recover_disk_tables(disk_table_names, opts, [disk_table | acc])

      {:error, :invalid_magic} ->
        case migrate(disk_table_name, opts) do
          {:ok, disk_table} ->
            recover_disk_tables(disk_table_names, opts, [disk_table | acc])

          error ->
            error
        end

      error ->
        error
    end
  end

  defp migrate(name, opts) do
    next_file_f = fn -> {tmp_file(name), name} end

    Logger.info(fn -> "Migrating #{name} to newer version" end)

    with {:ok, level_key, compressed?, iterator} <- Legacy.Iterator.new(name),
         {:ok, [disk_table]} <-
           DiskTable.write_new(
             Goblin.Iterator.linear_stream(fn -> iterator end),
             next_file_f,
             Keyword.merge(opts,
               level_key: level_key,
               compress?: compressed?,
               max_sst_size: :infinity
             )
           ) do
      Logger.info(fn -> "Migrated #{name} to newer version" end)
      {:ok, disk_table}
    end
  end

  defp tmp_file(file), do: "#{file}.tmp"

  defp file_path(dir, number) do
    name =
      number
      |> Integer.to_string(16)
      |> String.pad_leading(20, "0")

    Path.join(dir, "#{name}#{@file_suffix}")
  end
end
