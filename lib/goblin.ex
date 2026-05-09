defmodule Goblin do
  use GenServer

  alias Goblin.{
    MemTable,
    DiskTable,
    Manifest
  }

  defstruct [
    :data_dir,
    :mem_table,
    :compactor,
    :view,
    :manifest,
    :file_counter,
    levels: %{}
  ]

  def start_link(opts) do
    name = opts[:name] || __MODULE__
    args = Keyword.put_new(opts, :name, name)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl GenServer
  def init(args) do
    data_dir = args[:data_dir] || raise ":data_dir required"
    file_counter = :atomics.new(1, signed: false)

    case Manifest.open(data_dir) do
      {:ok, manifest} ->
        {:ok,
         %__MODULE__{
           data_dir: data_dir,
           file_counter: file_counter,
           manifest: manifest
         }, {:continue, :recover}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_continue(:recover, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:transaction, callback}, _from, state) do
    tx = %{}

    case callback.(tx) do
      {:commit, tx, reply} ->
        commits = Enum.reverse(tx.commits)

        case apply_commits(state, commits) do
          {:ok, state} -> {:reply, reply, state}
          {:error, reason} = error -> {:stop, reason, error, state}
        end

      :abort ->
        {:reply, {:error, :aborted}, state}
    end
  end

  defp apply_commits(commits, state) do
    case MemTable.append(state.mem_table, commits) do
      {:ok, mem_table} ->
        {:ok, %{state | mem_table: mem_table}}

      {:ok, old_mt, new_mt} ->
        job_ref =
          old_mt
          |> create_flush_job()
          |> start_job()

        {:ok, %{state | mem_table: new_mt}}
    end
  end

  defp create_flush_job(mt) do
    fn ->
      stream = MemTable.stream(mt)

      with {:ok, dts} <- DiskTable.build(stream, []) do
        {:ok, dts, [mt]}
      end
    end
  end

  defp start_job(job) do
    %{ref: ref} = Task.async(job)
    ref
  end
end
