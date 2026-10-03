defmodule Goblin.Server do
  @moduledoc false

  alias Goblin.DiskTable
  alias Goblin.Export
  alias Goblin.Levels
  alias Goblin.Merge
  alias Goblin.MVCC
  alias Goblin.Manifest
  alias Goblin.MemTable
  alias Goblin.WAL
  alias Goblin.Tx

  @behaviour :gen_statem

  @goblin_suffix "goblin"
  @wal_suffix "wal"

  @default_flush_level_file_limit 4
  @default_mem_limit 64 * 1024 * 1024
  @default_level_base_size 256 * 1024 * 1024
  @default_level_size_multiplier 10
  @default_fpp 0.01

  defstruct [
    :data_dir,
    :mem_table,
    :mvcc,
    :manifest,
    :file_counter,
    :opts,
    :writer,
    :wal,
    :compacting,
    :flushing,
    mem: 0,
    sqn: 0,
    levels: %{},
    flush_queue: :queue.new()
  ]

  @type t :: :gen_statem.server_ref()

  @spec start_link(keyword()) :: :gen_statem.start_ret()
  def start_link(opts) do
    name = opts[:name] || Goblin

    with {:ok, db_opts, genstatem_opts} <- split_opts(opts) do
      :gen_statem.start_link({:local, name}, __MODULE__, db_opts, genstatem_opts)
    end
  end

  @spec start(keyword()) :: :gen_statem.start_ret()
  def start(opts) do
    name = opts[:name] || Goblin

    with {:ok, db_opts, genstatem_opts} <- split_opts(opts) do
      :gen_statem.start({:local, name}, __MODULE__, db_opts, genstatem_opts)
    end
  end

  @spec stop(t(), term(), timeout()) :: :ok
  def stop(db, reason, timeout), do: :gen_statem.stop(db, reason, timeout)

  @spec start_transaction(t(), reference()) :: :ok | {:error, term()}
  def start_transaction(db, tx_ref), do: command(db, {:start_tx, tx_ref})

  @spec commit_transaction(t(), reference(), Tx.t()) :: :ok | {:error, term()}
  def commit_transaction(db, tx_ref, tx), do: command(db, {:commit_tx, tx_ref, tx})

  @spec cancel_transaction(t(), reference()) :: :ok | {:error, term()}
  def cancel_transaction(db, tx_ref), do: command(db, {:cancel_tx, tx_ref})

  @spec flushing?(t(), non_neg_integer()) :: boolean()
  def flushing?(db, timeout), do: command(db, :flushing?, timeout)

  @spec compacting?(t(), non_neg_integer()) :: boolean()
  def compacting?(db, timeout), do: command(db, :compacting?, timeout)

  @spec export(t(), Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def export(db, export_dir), do: command(db, {:export, export_dir})

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: opts[:name] || Goblin,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @impl :gen_statem
  def callback_mode, do: :state_functions

  @impl :gen_statem
  def terminate(_reason, _state, db) do
    with {_task_ref, task, _dts} <- db.compacting, do: Task.shutdown(task)
    with {_task_ref, task, _mt, _wal} <- db.flushing, do: Task.shutdown(task)
    :persistent_term.erase({Goblin, self()})
    db.manifest && Manifest.close(db.manifest)
    db.wal && WAL.close(db.wal)
    :ok
  end

  @impl :gen_statem
  def init(args) do
    Process.flag(:trap_exit, true)
    data_dir = args[:data_dir]

    opts =
      args
      |> Keyword.put_new(:bf_fpp, @default_fpp)
      |> Keyword.put_new(:mem_limit, @default_mem_limit)
      |> Keyword.put_new(:flush_level_file_limit, @default_flush_level_file_limit)
      |> Keyword.put_new(:level_base_size, @default_level_base_size)
      |> Keyword.put_new(:level_size_multiplier, @default_level_size_multiplier)
      |> Keyword.put_new(
        :max_sst_size,
        div(
          args[:level_base_size] || @default_level_base_size,
          args[:level_size_multiplier] || @default_level_size_multiplier
        )
      )

    db = %__MODULE__{
      data_dir: data_dir,
      file_counter: :atomics.new(1, signed: false),
      mvcc: MVCC.new(),
      opts: opts
    }

    with :ok <- File.mkdir_p(data_dir),
         {:ok, manifest} <- Manifest.open(data_dir),
         {:ok, db} <- recover(%{db | manifest: manifest}) do
      :persistent_term.put({Goblin, self()}, db.mvcc)
      {:ok, :idle, db, [{:next_event, :internal, :maybe_compact}]}
    end
  end

  @doc false
  def idle({:call, {pid, _} = from}, {:start_tx, tx_ref}, db) do
    ref = Process.monitor(pid)
    writer = {tx_ref, ref, pid}
    {:next_state, :occupied, %{db | writer: writer}, [{:reply, from, :ok}]}
  end

  def idle(type, event, db), do: handle_event(type, event, db)

  @doc false
  def occupied(:internal, :maybe_flush, db) do
    case maybe_flush(db) do
      {:ok, db} -> {:next_state, :idle, db}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  def occupied({:call, {pid, _} = from}, {:start_tx, _}, %{writer: {_, _, pid}} = db) do
    {:keep_state, db, [{:reply, from, {:error, :nested_transaction}}]}
  end

  def occupied({:call, _from}, {:start_tx, _}, db) do
    {:keep_state, db, [:postpone]}
  end

  def occupied({:call, from}, {:commit_tx, tx_ref, tx}, %{writer: {tx_ref, ref, _}} = db) do
    Process.demonitor(ref, [:flush])

    new_sqn = tx.sqn

    case WAL.append(db.wal, tx.commits) do
      {:ok, size} ->
        MemTable.append(db.mem_table, tx.commits)
        MVCC.update_sequence(db.mvcc, new_sqn)

        {:keep_state, %{db | sqn: new_sqn, mem: db.mem + size, writer: nil},
         [{:reply, from, :ok}, {:next_event, :internal, :maybe_flush}]}

      {:error, reason} = error ->
        {:stop_and_reply, reason, db, [{:reply, from, error}]}
    end
  end

  def occupied({:call, from}, {:cancel_tx, tx_ref}, %{writer: {tx_ref, ref, _}} = db) do
    Process.demonitor(ref, [:flush])
    {:next_state, :idle, %{db | writer: nil}, [{:reply, from, :ok}]}
  end

  def occupied(:info, {:DOWN, ref, _, _, _}, %{writer: {tx_ref, ref, _}} = db) do
    MVCC.unpin(db.mvcc, tx_ref)
    {:next_state, :idle, %{db | writer: nil}}
  end

  def occupied(type, event, db), do: handle_event(type, event, db)

  defp handle_event(:internal, :sweep, db) do
    to_delete = MVCC.sweep(db.mvcc)

    case delete_obsolete(to_delete) do
      :ok -> {:keep_state, db}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  defp handle_event(:internal, :maybe_compact, db) do
    {:keep_state, maybe_compact(db)}
  end

  defp handle_event({:call, from}, {:commit_tx, _, _}, db) do
    {:keep_state, db, [{:reply, from, {:error, :not_writer}}]}
  end

  defp handle_event({:call, from}, {:cancel_tx, _}, db) do
    {:keep_state, db, [{:reply, from, {:error, :not_writer}}]}
  end

  defp handle_event({:call, from}, {:export, export_dir}, db) do
    reply = Export.into_tar(export_dir, Manifest.snapshot(db.manifest))
    {:keep_state, db, [{:reply, from, reply}]}
  end

  defp handle_event({:call, from}, :flushing?, db) do
    {:keep_state, db, [{:reply, from, not is_nil(db.flushing)}]}
  end

  defp handle_event({:call, from}, :compacting?, db) do
    {:keep_state, db, [{:reply, from, not is_nil(db.compacting)}]}
  end

  defp handle_event(:info, {ref, {:ok, dts}}, %{flushing: {ref, _, _, _}} = db) do
    case finish_flush(db, dts) do
      {:ok, db} ->
        {:keep_state, db,
         [{:next_event, :internal, :maybe_compact}, {:next_event, :internal, :sweep}]}

      {:error, reason} ->
        {:stop, reason, db}
    end
  end

  defp handle_event(:info, {ref, {:error, reason}}, %{flushing: {ref, _, _, _}} = db) do
    {:stop, reason, db}
  end

  defp handle_event(:info, {ref, {:ok, dts}}, %{compacting: {ref, _, _}} = db) do
    case finish_compaction(db, dts) do
      {:ok, db} ->
        {:keep_state, db,
         [{:next_event, :internal, :maybe_compact}, {:next_event, :internal, :sweep}]}

      {:error, reason} ->
        {:stop, reason, db}
    end
  end

  defp handle_event(:info, {ref, {:error, reason}}, %{compacting: {ref, _, _}} = db) do
    {:stop, reason, db}
  end

  defp handle_event(:info, {:DOWN, ref, _, _, reason}, %{flushing: {ref, _, _, _}} = db) do
    {:stop, reason, db}
  end

  defp handle_event(:info, {:DOWN, ref, _, _, reason}, %{compacting: {ref, _, _}} = db) do
    {:stop, reason, db}
  end

  defp handle_event(_, _, db), do: {:keep_state, db}

  defp command(db, cmd, timeout \\ :infinity) do
    :gen_statem.call(db, cmd, timeout)
  end

  defp recover(db) do
    snapshot = Manifest.snapshot(db.manifest)
    data_files = Enum.filter(snapshot, &String.ends_with?(&1, [@wal_suffix, @goblin_suffix]))
    {wals, dts} = Enum.split_with(data_files, &String.ends_with?(&1, @wal_suffix))
    max_count = data_files |> Enum.map(&get_count_from_file/1) |> Enum.max(fn -> 0 end)
    :atomics.put(db.file_counter, 1, max_count + 1)

    with :ok <- delete_inactive(db.data_dir, snapshot),
         {:ok, db} <- load_wals(db, wals),
         {:ok, db} <- load_disk_tables(db, dts),
         {:ok, db} <- open_wal_and_mem_table(db) do
      sqn = db.sqn + 1
      mts = [db.mem_table | for({mt, _wal} <- :queue.to_list(db.flush_queue), do: mt)]
      dts = db.levels |> Map.values() |> List.flatten()
      MVCC.put_version(db.mvcc, mts ++ dts, [])
      MVCC.update_sequence(db.mvcc, sqn)
      {:ok, dequeue_flush(%{db | sqn: sqn})}
    end
  end

  defp load_wals(db, []), do: {:ok, db}

  defp load_wals(db, [wal | wals]) do
    with {:ok, wal} <- WAL.open(wal),
         {:ok, db} <- load_wal(db, wal) do
      load_wals(db, wals)
    end
  end

  defp load_wal(db, wal) do
    mt = MemTable.new()
    logs = WAL.stream(wal)

    with {:ok, sqn} <-
           Enum.reduce_while(logs, {:ok, -1}, fn
             {:ok, commits}, {:ok, sqn} -> {:cont, {:ok, max(sqn, MemTable.append(mt, commits))}}
             error, _acc -> {:halt, error}
           end) do
      %{db | sqn: max(db.sqn, sqn)}
      |> enqueue_flush(mt, wal)
    end
  end

  defp load_disk_tables(db, []), do: {:ok, db}

  defp load_disk_tables(db, [dt | dts]) do
    with {:ok, %{sqn_range: {_min, max_sqn}} = dt} <- DiskTable.from_file(dt) do
      levels = Levels.put(db.levels, dt)
      db = %{db | levels: levels, sqn: max(db.sqn, max_sqn)}
      load_disk_tables(db, dts)
    end
  end

  defp open_wal_and_mem_table(db) do
    wal_path = gen_file(db.data_dir, db.file_counter, @wal_suffix)
    mt = MemTable.new()

    with {:ok, wal} <- WAL.open(wal_path, true),
         {:ok, manifest} <- Manifest.update(db.manifest, [wal_path], []) do
      {:ok, %{db | wal: wal, mem_table: mt, manifest: manifest}}
    end
  end

  defp finish_flush(db, new_dts) do
    {_task_ref, _task, mt, wal} = db.flushing
    new_dts_ids = Enum.map(new_dts, & &1.id)

    with {:ok, manifest} <- Manifest.update(db.manifest, new_dts_ids, [wal.id]),
         :ok <- WAL.delete(wal) do
      levels = Enum.reduce(new_dts, db.levels, &Levels.put(&2, &1))
      MVCC.put_version(db.mvcc, new_dts, [mt])
      {:ok, %{db | manifest: manifest, levels: levels, flushing: nil} |> dequeue_flush()}
    end
  end

  defp finish_compaction(db, new_dts) do
    {_task_ref, _task, old_dts} = db.compacting
    new_dts_ids = Enum.map(new_dts, & &1.id)
    old_dts_ids = Enum.map(old_dts, & &1.id)

    with {:ok, manifest} <- Manifest.update(db.manifest, new_dts_ids, old_dts_ids) do
      levels = Enum.reduce(new_dts, db.levels, &Levels.put(&2, &1))
      MVCC.put_version(db.mvcc, new_dts, old_dts)
      {:ok, %{db | manifest: manifest, levels: levels, compacting: nil}}
    end
  end

  defp maybe_flush(db) do
    if db.mem >= db.opts[:mem_limit],
      do: rotate(db),
      else: {:ok, db}
  end

  defp maybe_compact(%{compacting: nil} = db) do
    case Levels.next(db.levels, db.opts) do
      nil ->
        db

      {:merge, lk, dts, filter_tombstones?, levels} ->
        %{db | levels: levels}
        |> compact(lk, dts, filter_tombstones?)
    end
  end

  defp maybe_compact(db), do: db

  defp rotate(db) do
    with {:ok, db} <- enqueue_flush(db, db.mem_table, db.wal),
         {:ok, db} <- open_wal_and_mem_table(db) do
      MVCC.put_version(db.mvcc, [db.mem_table], [])
      {:ok, dequeue_flush(db)}
    end
  end

  defp enqueue_flush(db, mt, wal) do
    with :ok <- WAL.close(wal) do
      {:ok, %{db | mem: 0, flush_queue: :queue.in({mt, wal}, db.flush_queue)}}
    end
  end

  defp dequeue_flush(%{flushing: nil} = db) do
    case :queue.out(db.flush_queue) do
      {:empty, _} -> db
      {{:value, {mt, wal}}, flush_queue} -> flush(%{db | flush_queue: flush_queue}, mt, wal)
    end
  end

  defp dequeue_flush(db), do: db

  defp flush(db, mt, wal) do
    task = merge_task(fn -> MemTable.stream(mt) end, build_opts(db, 0))
    %{db | flushing: {task.ref, task, mt, wal}}
  end

  defp compact(db, lk, dts, filter_tombstones?) do
    task =
      merge_task(
        fn ->
          Merge.stream(fn -> Enum.map(dts, &DiskTable.stream/1) end,
            filter_tombstones?: filter_tombstones?
          )
        end,
        build_opts(db, lk)
      )

    %{db | compacting: {task.ref, task, dts}}
  end

  defp merge_task(stream_fun, opts),
    do: Task.async(fn -> build_tables(stream_fun.(), opts) end)

  defp build_tables(stream, opts) do
    DiskTable.build(stream, opts)
  rescue
    e in Goblin.IOError -> {:error, e}
  end

  defp build_opts(db, lk) do
    %{data_dir: data_dir, file_counter: file_counter} = db

    [
      level_key: lk,
      compress?: lk > 1,
      max_size: db.opts[:max_sst_size],
      fpp: db.opts[:bf_fpp],
      filer: fn -> gen_file(data_dir, file_counter) end
    ]
  end

  defp delete_obsolete([]), do: :ok

  defp delete_obsolete([%MemTable{} = mt | tabs]) do
    MemTable.delete(mt)
    delete_obsolete(tabs)
  end

  defp delete_obsolete([%DiskTable{} = dt | tabs]) do
    with :ok <- DiskTable.delete(dt) do
      delete_obsolete(tabs)
    end
  end

  defp gen_file(dir, file_counter, suffix \\ @goblin_suffix) do
    prefix =
      (:atomics.add_get(file_counter, 1, 1) - 1)
      |> Integer.to_string(16)
      |> String.pad_leading(20, "0")

    Path.join(dir, "#{prefix}.#{suffix}")
  end

  defp delete_inactive(dir, active) do
    with {:ok, all} <- File.ls(dir) do
      all
      |> Enum.filter(&String.ends_with?(&1, [@wal_suffix, @goblin_suffix]))
      |> Enum.map(&Path.join(dir, &1))
      |> Enum.reject(&(&1 in active))
      |> delete()
    end
  end

  defp delete([]), do: :ok
  defp delete([path | paths]), do: with(:ok <- File.rm(path), do: delete(paths))

  defp get_count_from_file(path) do
    [count_s, _suffix] =
      path
      |> Path.basename()
      |> String.split(".")

    String.to_integer(count_s, 16)
  end

  defp split_opts(opts) do
    {gen_statem_opts, db_opts} =
      Keyword.split(opts, [:timeout, :spawn_opt, :hibernate_after, :debug])

    case Keyword.get(db_opts, :data_dir) do
      nil ->
        {:error, :data_dir_not_provided}

      data_dir ->
        try do
          {:ok, Keyword.put(db_opts, :data_dir, to_string(data_dir)), gen_statem_opts}
        rescue
          Protocol.UndefinedError ->
            {:error, :data_dir_not_a_string}
        end
    end
  end
end
