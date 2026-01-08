defmodule Goblin.DiskTables do
  @moduledoc false
  use GenServer
  alias Goblin.DiskTables.{DiskTable, BinarySearchIterator, StreamIterator}
  alias Goblin.BloomFilter
  alias Goblin.Manifest
  alias Goblin.Compactor

  @file_suffix ".goblin"

  defstruct [
    :dir,
    :table,
    :manifest_server,
    :compactor_server,
    :opts,
    file_number: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]
    local_name = opts[:local_name] || name

    args =
      opts
      |> Keyword.take([
        :db_dir,
        :manifest_server,
        :compactor_server,
        :bf_fpp,
        :max_sst_size
      ])
      |> Keyword.put(:local_name, local_name)

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec new(Goblin.server(), Enumerable.t(Goblin.triple()), keyword()) ::
          {:ok, [Goblin.db_file()]} | {:error, term()}
  def new(server, stream, opts) do
    next_file_f = fn ->
      file = new_file(server)
      {tmp_file(file), file}
    end

    opts = Keyword.merge(opts, GenServer.call(server, :get_opts))

    with {:ok, disk_tables} <- DiskTable.write_new(stream, next_file_f, opts),
         :ok <- GenServer.call(server, {:put, disk_tables}) do
      {:ok, Enum.map(disk_tables, & &1.file)}
    end
  end

  @spec remove(Goblin.server(), Goblin.db_file()) :: :ok | {:error, term()}
  def remove(server, file) do
    GenServer.call(server, {:remove, file})
  end

  @spec search_iterators(Goblin.table(), [Goblin.db_key()], Goblin.seq_no()) :: [
          Goblin.Iterable.t()
        ]
  def search_iterators(ets, keys, seq) do
    Enum.reduce(keys, %{}, fn key, acc ->
      guard = [
        {:andalso, {:"=<", :"$1", key}, {:"=<", key, :"$2"}}
      ]

      ms = [{{:_, {:"$1", :"$2"}, :"$3"}, guard, [:"$3"]}]

      :ets.select(ets, ms)
      |> Enum.filter(fn disk_table ->
        BloomFilter.member?(disk_table.bloom_filter, key)
      end)
      |> Enum.reduce(acc, fn disk_table, acc ->
        Map.update(acc, disk_table, [key], &[key | &1])
      end)
    end)
    |> Enum.map(fn {disk_table, keys} ->
      BinarySearchIterator.new(disk_table, keys, seq)
    end)
  end

  @spec stream_iterators(Goblin.table(), Goblin.db_key(), Goblin.db_key(), Goblin.seq_no()) :: [
          Goblin.Iterable.t()
        ]
  def stream_iterators(ets, min, max, seq) do
    guard =
      cond do
        is_nil(min) and is_nil(max) -> []
        is_nil(min) -> [{:"=<", :"$2", max}]
        is_nil(max) -> [{:"=<", min, :"$3"}]
        true -> [{:andalso, {:"=<", :"$2", max}, {:"=<", min, :"$3"}}]
      end

    ms = [{{:"$1", {:"$2", :"$3"}, :_}, guard, [:"$1"]}]

    :ets.select(ets, ms)
    |> Enum.map(&StreamIterator.new(&1, seq))
  end

  @spec iterator(Goblin.db_file()) :: Goblin.Iterable.t()
  def iterator(file) do
    StreamIterator.new(file)
  end

  @spec new_file(Goblin.server()) :: {Goblin.db_file(), Goblin.db_file()}
  def new_file(server) do
    GenServer.call(server, :new_file)
  end

  @impl GenServer
  def init(args) do
    table = :ets.new(args[:local_name], [:named_table])

    {:ok,
     %__MODULE__{
       dir: args[:db_dir],
       table: table,
       manifest_server: args[:manifest_server],
       compactor_server: args[:compactor_server],
       opts: [bf_fpp: args[:bf_fpp], max_sst_size: args[:max_sst_size]]
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:put, disk_tables}, _from, state) do
    Enum.each(disk_tables, fn disk_table ->
      :ets.insert(
        state.table,
        {disk_table.file, disk_table.key_range, disk_table}
      )

      Compactor.put(state.compactor_server, disk_table)
    end)

    {:reply, :ok, state}
  end

  def handle_call({:remove, file}, _from, state) do
    :ets.delete(state.table, file)
    {:reply, :ok, state}
  end

  def handle_call(:new_file, _from, state) do
    new_file = file_path(state.dir, state.file_number)
    {:reply, new_file, %{state | file_number: state.file_number + 1}}
  end

  def handle_call(:get_opts, _from, state) do
    {:reply, state.opts, state}
  end

  @impl GenServer
  def handle_continue(:recover_state, state) do
    %{disk_tables: disk_table_names, count: count} =
      Manifest.snapshot(state.manifest_server, [:disk_tables, :count])

    case recover_disk_tables(disk_table_names) do
      {:ok, disk_tables} ->
        Enum.each(disk_tables, fn disk_table ->
          :ets.insert(
            state.table,
            {disk_table.file, disk_table.key_range, disk_table}
          )

          Compactor.put(state.compactor_server, disk_table)
        end)

        :ets.insert(state.table, {:ready})
        {:noreply, %{state | file_number: count}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp recover_disk_tables(disk_table_names, acc \\ [])
  defp recover_disk_tables([], acc), do: {:ok, acc}

  defp recover_disk_tables([disk_table_name | disk_table_names], acc) do
    with {:ok, disk_table} <- DiskTable.parse(disk_table_name) do
      recover_disk_tables(disk_table_names, [disk_table | acc])
    end
  end

  defp tmp_file(file), do: "#{file}.tmp"
  defp file_path(dir, number), do: Path.join(dir, "#{to_string(number)}#{@file_suffix}")
end
