# Goblin

A lightweight, embedded database for Elixir.

## Features

- **ACID transactions** with serial write execution
- **Concurrent readers** that do not block each other
- **Crash recovery** via write-ahead logging
- **Automatic background compaction**
- **Bloom filters** for fast negative lookups
- **Any Elixir term** as key or value

## Usage

Install by adding `:goblin` as a dependency:
```elixir
def deps do
  [
    {:goblin, "~> 0.6.0"}
  ]
end
```
Then run `mix deps.get`.

### Starting a database

```elixir
{:ok, db} = Goblin.start_link(
  name: MyApp.DB,
  data_dir: "/path/to/db"
)
```

Options:
- `:name` - Registered name for the database (optional, defaults to `Goblin`)
- `:data_dir` - Directory path for database files (required)
- `:mem_limit` - Bytes to buffer in memory before flushing to disk (default: 64 MB)
- `:bf_fpp` - Bloom filter false positive probability (default: 0.01)

### Basic operations

```elixir
Goblin.put(db, :alice, "Alice")
# => :ok

Goblin.get(db, :alice)
# => "Alice"

Goblin.get(db, :nonexistent)
# => nil

Goblin.get(db, :nonexistent, default: :not_found)
# => :not_found

Goblin.remove(db, :alice)
# => :ok
```

> #### Note {: .warning}
>
> A key can be any Elixir term, but avoid mixing floats and integers as keys
> (e.g. both `1` and `1.0`). This can cause unexpected reads.

### Batch operations

```elixir
Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}])
# => :ok

Goblin.get_multi(db, [:alice, :bob, :charlie])
# => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

Goblin.remove_multi(db, [:bob, :charlie])
# => :ok
```

### Range queries

```elixir
Goblin.select(db) |> Enum.to_list()
# => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

Goblin.select(db, min: :bob) |> Enum.to_list()
# => [{:bob, "Bob"}, {:charlie, "Charlie"}]

Goblin.select(db, min: :alice, max: :bob) |> Enum.to_list()
# => [{:alice, "Alice"}, {:bob, "Bob"}]
```

### Transactions

There are two types of transactions: **write** and **read**.

Write transactions (`Goblin.transaction/2`) are executed serially.
The function must return `{:commit, tx, reply}` to commit or `:abort` to abort.

```elixir
Goblin.transaction(db, fn tx ->
  counter = Goblin.Tx.get(tx, :counter, default: 0)
  tx = Goblin.Tx.put(tx, :counter, counter + 1)
  {:commit, tx, :ok}
end)
# => :ok

Goblin.transaction(db, fn tx ->
  counter = Goblin.Tx.get(tx, :counter, default: 0)

  if counter < 100 do
    tx = Goblin.Tx.put(tx, :counter, counter + 1)
    {:commit, tx, counter + 1}
  else
    :abort
  end
end)
# => 2 (if committed) or {:error, :aborted} (if aborted)
```

Read transactions (`Goblin.read/2`) take a snapshot and do not block each other.
Attempting to write within a read transaction raises.

```elixir
Goblin.read(db, fn tx ->
  alice = Goblin.Tx.get(tx, :alice)
  bob = Goblin.Tx.get(tx, :bob)
  {alice, bob}
end)
# => {"Alice", "Bob"}
```

### Tags

Keys can be namespaced under a tag. Tagged and untagged data are separate:
reading without a tag returns only untagged data, and reading with a tag
returns only data under that tag.

```elixir
Goblin.put(db, :alice, "Alice", tag: :admins)
# => :ok

Goblin.get(db, :alice)
# => nil

Goblin.get(db, :alice, tag: :admins)
# => "Alice"

Goblin.select(db, tag: :admins) |> Enum.to_list()
# => [{:alice, "Alice"}]

# Removing without a tag does not affect tagged data
Goblin.remove(db, :alice)
Goblin.get(db, :alice, tag: :admins)
# => "Alice"

# Remove with the matching tag
Goblin.remove(db, :alice, tag: :admins)
Goblin.get(db, :alice, tag: :admins)
# => nil
```

### PubSub

Processes can subscribe to write notifications. On each committed write,
subscribers receive `{:put, key, value}` or `{:remove, key}` messages.

```elixir
Goblin.subscribe(db)
# => :ok

Goblin.put(db, :alice, "Alice")
# subscriber receives {:put, :alice, "Alice"}

Goblin.remove(db, :alice)
# subscriber receives {:remove, :alice}

Goblin.unsubscribe(db)
# => :ok
```

### Backups

A snapshot of the database can be exported as a `.tar.gz` archive.
The archive can be unpacked and used as the `data_dir` for a new instance.

```elixir
Goblin.export(db, "/backups")
# => {:ok, "/backups/goblin_20260220T120000Z.tar.gz"}
```

### Supervision tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Goblin, name: MyApp.DB, data_dir: "/var/lib/myapp/db"}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

Goblin.put(MyApp.DB, :alice, "Alice")
# => :ok

Goblin.get(MyApp.DB, :alice)
# => "Alice"
```

## Disk table format

Database files use the `<number>.goblin` naming convention.

| SST | SEPARATOR | FOOTER |
| --- | --- | --- |
| n * 1024 bytes | 16 bytes  | variable size |

### Sorted String Table (SST)

The SST consists of multiple blocks.
Each block is a multiple of 1024 bytes and contains:
- **Block ID** (16 bytes): `"GOBLINBLOCK00000"`
- **Span** (2 bytes): Number of blocks this entry spans
- **Data** (variable): Erlang term encoding of `{sequence, key, value}`
- **Padding** (variable): Zero-filled to reach n * 1024 bytes

### Footer

The footer contains metadata for efficient lookups:
- **Separator** (16 bytes): `"GOBLINSEP0000000"`
- **Bloom filter** (variable): Encoded bloom filter for membership testing
- **Key range** (variable): Min and max keys in the SST
- **Sequence range** (variable): Lowest and highest sequence numbers
- **Metadata** (57 bytes): Positions and sizes of footer components
- **CRC** (32-bit): Checksum over SST, bloom filter, key range, sequence range and metadata
- **Magic** (16 bytes): `"GOBLINFILE000000"`

### Migration

If a previous version of the disk table format is detected on startup,
the file is automatically migrated to the current version.

## Too many open files?

If you see `:emfile` errors, the file descriptor limit has been reached.
Increase it with `ulimit -n <limit>`.
