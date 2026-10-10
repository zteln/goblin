defmodule Goblin.Server do
  @moduledoc false

  alias Goblin.DiskTable
  alias Goblin.Export
  alias Goblin.Compaction
  alias Goblin.Merge
  alias Goblin.MVCC
  alias Goblin.Manifest
  alias Goblin.MemTable
  alias Goblin.WAL
  alias Goblin.Tx

  @behaviour :gen_statem

  @goblin_suffix "goblin"
  @wal_suffix "wal"

  @sweep_interval 1_000

  @default_flush_level_file_limit 4
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
    mem: 0,
    jobs: %{},
    flush_queue: :queue.new()
  ]

  @type t :: pid() | atom()

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
  def flushing?(db, timeout), do: command(db, {:check_job, :flush}, timeout)

  @spec compacting?(t(), non_neg_integer()) :: boolean()
  def compacting?(db, timeout), do: command(db, {:check_job, :compact}, timeout)

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
    for {_, {_, task, _, _}} <- db.jobs, do: Task.shutdown(task)
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
      |> Keyword.put_new(:flush_level_file_limit, @default_flush_level_file_limit)
      |> Keyword.put_new(:level_base_size, @default_level_base_size)
      |> Keyword.put_new(:level_size_multiplier, @default_level_size_multiplier)
      |> Keyword.put_new(
        :max_size,
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

    with :ok <- validate_fpp(opts[:bf_fpp]),
         :ok <- File.mkdir_p(data_dir),
         {:ok, manifest} <- Manifest.open(data_dir),
         {:ok, db} <- recover(%{db | manifest: manifest}) do
      :persistent_term.put({Goblin, self()}, db.mvcc)

      {:ok, :idle, db,
       [{:next_event, :internal, :maybe_compact}, {{:timeout, :sweep}, @sweep_interval, :sweep}]}
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

  def occupied(
        {:call, from},
        {:commit_tx, tx_ref, %Tx{ref: tx_ref} = tx},
        %{writer: {tx_ref, ref, _}} = db
      ) do
    Process.demonitor(ref, [:flush])

    new_sqn = tx.sqn

    case WAL.append(db.wal, tx.commits) do
      {:ok, size} ->
        MemTable.append(db.mem_table, tx.commits)
        MVCC.update_sequence(db.mvcc, new_sqn)

        {:keep_state, %{db | mem: db.mem + size, writer: nil},
         [{:reply, from, :ok}, {:next_event, :internal, :maybe_flush}]}

      {:error, reason} = error ->
        {:stop_and_reply, reason, db, [{:reply, from, error}]}
    end
  end

  def occupied({:call, from}, {:commit_tx, tx_ref, _}, %{writer: {tx_ref, _, _}} = db) do
    Process.demonitor(ref, [:flush])
    {:next_state, :idle, %{db | writer: nil}, [{:reply, from, {:error, :invalid_tx}}]}
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

  defp handle_event(:internal, :maybe_compact, db) do
    {:keep_state, maybe_compact(db)}
  end

  defp handle_event({:timeout, :sweep}, :sweep, db) do
    case delete_obsolete(MVCC.sweep(db.mvcc)) do
      :ok -> {:keep_state, db, [{{:timeout, :sweep}, @sweep_interval, :sweep}]}
      {:error, reason} -> {:stop, reason, db}
    end
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

  defp handle_event({:call, from}, {:check_job, kind}, db) do
    {:keep_state, db, [{:reply, from, running?(db, kind)}]}
  end

  defp handle_event({:call, from}, _, db) do
    {:keep_state, db, [{:reply, from, {:error, :invalid_call}}]}
  end

  defp handle_event(:info, {ref, {:ok, dts}}, %{jobs: jobs} = db) when is_map_key(jobs, ref) do
    {job, jobs} = Map.pop(jobs, ref)

    case install(%{db | jobs: jobs}, job, dts) do
      {:ok, db} -> {:keep_state, db, [{:next_event, :internal, :maybe_compact}]}
      {:error, reason} -> {:stop, reason, db}
    end
  end

  defp handle_event(:info, {ref, {:error, reason}}, %{jobs: jobs} = db)
       when is_map_key(jobs, ref), do: {:stop, reason, db}

  defp handle_event(:info, {:DOWN, ref, _, _, reason}, %{jobs: jobs} = db)
       when is_map_key(jobs, ref), do: {:stop, reason, db}

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
         {:ok, db, sqn1, mts} <- load_wals(db, wals),
         {:ok, sqn2, dts} <- load_disk_tables(dts),
         {:ok, db} <- open_wal_and_mem_table(db) do
      mts = [db.mem_table | mts]
      MVCC.put_version(db.mvcc, mts ++ dts, [])
      MVCC.update_sequence(db.mvcc, max(sqn1, sqn2) + 1)
      {:ok, dequeue_flush(db)}
    end
  end

  defp load_wals(db, wals, sqn \\ -1, mts \\ [])
  defp load_wals(db, [], sqn, mts), do: {:ok, db, sqn, mts}

  defp load_wals(db, [wal | wals], sqn1, mts) do
    with {:ok, wal} <- WAL.open(wal),
         {:ok, db, sqn2, mt} <- load_wal(db, wal) do
      load_wals(db, wals, max(sqn1, sqn2), [mt | mts])
    end
  end

  defp load_wal(db, wal) do
    mt = MemTable.new()

    with {:ok, sqn} <- WAL.replay(wal, -1, &max(&2, MemTable.append(mt, &1))),
         {:ok, db} <- enqueue_flush(db, mt, wal) do
      {:ok, db, sqn, mt}
    end
  end

  defp load_disk_tables(dts, sqn \\ -1, acc \\ [])
  defp load_disk_tables([], sqn, acc), do: {:ok, sqn, acc}

  defp load_disk_tables([dt | dts], sqn1, acc) do
    with {:ok, %{sqn_range: {_min, max_sqn}} = dt} <- DiskTable.from_file(dt) do
      load_disk_tables(dts, max(sqn1, max_sqn), [dt | acc])
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

  defp install(db, {_kind, _task, old_tabs, wal}, new_dts) do
    new_ids = Enum.map(new_dts, & &1.id)
    old_ids = for %DiskTable{id: id} <- old_tabs, do: id
    removed = if wal, do: [wal.id | old_ids], else: old_ids

    with {:ok, manifest} <- Manifest.update(db.manifest, new_ids, removed),
         :ok <- if(wal, do: WAL.delete(wal), else: :ok) do
      MVCC.put_version(db.mvcc, new_dts, old_tabs)
      {:ok, dequeue_flush(%{db | manifest: manifest})}
    end
  end

  defp maybe_flush(db) do
    if db.mem >= db.opts[:max_size],
      do: rotate(db),
      else: {:ok, db}
  end

  defp maybe_compact(db) do
    if running?(db, :compact), do: db, else: compact_next(db)
  end

  defp compact_next(db) do
    pin_key = make_ref()
    MVCC.pin(db.mvcc, pin_key)
    tables = MVCC.get_all_tables(db.mvcc, pin_key)
    MVCC.unpin(db.mvcc, pin_key)

    case Compaction.next(tables, db.opts) do
      nil ->
        db

      {:merge, lk, dts, filter_tombstones?} ->
        start_job(
          db,
          :compact,
          fn ->
            Merge.stream(fn -> Enum.map(dts, &DiskTable.stream/1) end,
              filter_tombstones?: filter_tombstones?
            )
          end,
          build_opts(db, lk),
          dts
        )
    end
  end

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

  defp dequeue_flush(db) do
    if running?(db, :flush), do: db, else: flush_next(db)
  end

  defp flush_next(db) do
    case :queue.out(db.flush_queue) do
      {:empty, _} -> db
      {{:value, {mt, wal}}, flush_queue} -> flush(%{db | flush_queue: flush_queue}, mt, wal)
    end
  end

  defp flush(db, mt, wal) do
    start_job(
      db,
      :flush,
      fn -> MemTable.stream(mt) end,
      build_opts(db, 0, :infinity),
      [mt],
      wal
    )
  end

  defp start_job(db, kind, stream_fun, opts, old_tabs, wal \\ nil) do
    task = Task.async(fn -> build_tables(stream_fun.(), opts) end)
    %{db | jobs: Map.put(db.jobs, task.ref, {kind, task, old_tabs, wal})}
  end

  defp build_tables(stream, opts) do
    DiskTable.build(stream, opts)
  rescue
    e in Goblin.IOError -> {:error, e}
  end

  defp build_opts(db, lk, max_size \\ nil) do
    %{data_dir: data_dir, file_counter: file_counter} = db

    [
      level_key: lk,
      compress?: lk > 1,
      max_size: max_size || db.opts[:max_size],
      fpp: db.opts[:bf_fpp],
      filer: fn -> gen_file(data_dir, file_counter) end
    ]
  end

  defp running?(db, kind) do
    Enum.any?(db.jobs, fn {_ref, {k, _, _, _}} -> k == kind end)
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

  defp validate_fpp(fpp) when is_number(fpp) and 0 < fpp and fpp < 1, do: :ok
  defp validate_fpp(_), do: {:error, :invalid_fpp}

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
