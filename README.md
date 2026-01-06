# Goblin

An embedded LSM-Tree database for Elixir.

## What is an LSM-Tree?

A Log-Structured Merge-Tree (LSM-Tree) is a data structure optimized for write-heavy workloads. 
Instead of updating data in place, writes are first appended to a write-ahead log (WAL) for durability, then to an in-memory structure (MemTable).
When the memory buffer fills up, it's flushed to disk as an immutable sorted file (SST).
Reads check the memory first, then searches through the SSTs from newest to oldest.
Background compaction merges files on disk to reduce read amplification and reclaim space from deleted entries.
This design provides excellent write throughput while maintaining reasonable read performance through bloom filters and sorted data structures.

## Features

- **LSM-Tree architecture**: Write-optimized storage with efficient compaction
- **ACID transactions**: True serial execution of transactions
- **Concurrent reads**: Multiple readers can access data simultaneously
- **Write-ahead logging (WAL)**: Durability and crash recovery
- **Bloom filters**: Fast negative lookups to skip unnecessary SST scans
- **Automatic compaction**: Background merging of SST files across levels
- **Configurable limits**: Tune memory usage and compaction behavior

## Usage

Install by adding `:goblin` as a dependency:
```elixir
def deps do
  [
    {:goblin, "~> 0.4.0"}
  ]
end
```
Then run `mix deps.get`.

### Starting a database

```elixir
{:ok, db} = Goblin.start_link(
  name: MyApp.DB,
  db_dir: "/path/to/db",
  flush_level_file_limit: 4,
  mem_limit: 64 * 1024 * 1024,
  level_base_size: 256 * 1024 * 1024,
  level_size_multiplier: 10,
  bf_fpp: 0.05
)
```

Options:
- `name` - Registered name for the database supervisor (optional)
- `db_dir` - Directory path for storing database files (required)
- `flush_level_file_limit` - How many files in flush level before compaction is triggered (default: 4)
- `mem_limit` - How many bytes are stored in memory before flushing (default: 64 * 1024 * 1024)
- `level_base_size` - How many bytes are stored in level 1 before compaction is triggered (default: 256 * 1024 * 1024)
- `level_size_multiplier` - Which factor each level size is multiplied with (default: 10)
- `max_sst_size` - How large, in bytes, the SST portion of a disk table is allowed to be (default: level_base_size / level_size_multiplier)
- `bf_fpp` - Bloom filter false positive probability (default: 0.01)

> #### Note {: .info}
>
> Measuring the memory size of the MemTable in bytes is done via `:ets.info(mem_table, :memory) * :erlang.system_info(:wordsize)`. This might not correspond exactly to the amount of bytes stored. This can thus produce approximate sizes.

### Basic operations

```elixir
Goblin.put(db, :user_123, %{name: "Alice", age: 30})
# => :ok

Goblin.get(db, :user_123)
# => %{name: "Alice", age: 30}

Goblin.get(db, :nonexistent)
# => nil

Goblin.get(db, :nonexistent, :not_found)
# => :not_found

Goblin.remove(db, :user_123)
# => :ok
```

### Batch operations

```elixir
Goblin.put_multi(db, [
  {:user_1, %{name: "Alice"}},
  {:user_2, %{name: "Bob"}},
  {:user_3, %{name: "Charlie"}}
])
# => :ok

Goblin.get_multi(db, [:user_1, :user_2, :user_3])
# => [{:user_1, %{name: "Alice"}}, {:user_2, %{name: "Bob"}}, {:user_3, %{name: "Charlie"}}]

Goblin.remove_multi(db, [:user_1, :user_2])
# => :ok
```

### Range queries

```elixir
Goblin.put_multi(db, [
  {1, "one"},
  {2, "two"},
  {3, "three"},
  {4, "four"},
  {5, "five"}
])

Goblin.select(db, min: 2, max: 4) |> Enum.to_list()
# => [{2, "two"}, {3, "three"}, {4, "four"}]

Goblin.select(db, min: 3) |> Enum.to_list()
# => [{3, "three"}, {4, "four"}, {5, "five"}]

Goblin.select(db, max: 2) |> Enum.to_list()
# => [{1, "one"}, {2, "two"}]
```

### Transactions

```elixir
alias Goblin.Tx

Goblin.transaction(db, fn tx ->
  count = Tx.get(tx, :counter, 0)
  tx = Tx.put(tx, :counter, count + 1)
  {:commit, tx, count + 1}
end)
# => 1

Goblin.transaction(db, fn tx ->
  balance = Tx.get(tx, :account_balance, 0)
  
  if balance >= 100 do
    tx = Tx.put(tx, :account_balance, balance - 100)
    {:commit, tx, :ok}
  else
    :abort
  end
end)
# => :ok (if committed) or {:error, :aborted} (if aborted)

Goblin.read(db, fn tx -> 
  root = Tx.get(tx, :root)
  next = Tx.get(tx, next) 
  Tx.get(tx, next)
end)
# => next
```

There are two types of transactions: read and write.

A read transaction (via `Goblin.read/2`) do not block each other and are not allowed to write anything (causes a raise).

Write transactions (via `Goblin.transaction/2`) are executed in serial order. 
Writers are queued and are dequeued until the current writer completes its transaction.

### PubSub

Processes can subscribe to database writes.
When writes are committed to the database MemTable then subscribers receive either `{:put, key, value}` or `{:remove, key}`.

```elixir
Goblin.subscribe(db)
# => :ok

Goblin.unsubscribe(db)
# => :ok
```

### Back ups
A snapshot of the database can be exported in a `.tar.gz` file.
This file can be unpacked if the database needs to restored at a later time, acting as a backup.

```elixir
Goblin.export(db, "path/to/back_up_dir/")
# => {:ok, back_up}
```

### Using with a supervision tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Goblin, name: MyApp.DB, db_dir: "/var/lib/myapp/db"}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

Goblin.put(MyApp.DB, :key, "value")
# => :ok

Goblin.get(MyApp.DB, :key)
# => "value"
```

## How it works

Goblin implements a Log-Structured Merge-Tree (LSM-Tree) with the following components:

- **Broker** - Transactions management
- **MemTable** - Memory buffer server
- **DiskTables** - Disk table management (notation: a disk table consists of an SST + metadata)
- **Compactor** - Compaction server
- **Cleaner** - Safe file removal server
- **WAL** - Write-ahead log server
- **Manifest** - Tracks database state and file metadata

### Flushing

When the MemTable size exceeds `mem_limit`, then a flush is initiated:

1. **WAL rotation**: The current WAL file is rotated to preserve unflushed writes
2. **SST creation**: The selected entries up to a sequence number in the MemTable is sorted and written to one or more SST files at level 0 (flush level)
3. **Bloom filter generation**: A Bloom filter is created for each SST to enable fast negative lookups
4. **Manifest update**: The new disk table files are logged in the manifest
5. **WAL cleanup**: The rotated WAL file is deleted after a successful flush
6. **Disk table tracking**: Disk tables are tracked for later lookups
7. **MemTable cleanup**: The flushed entries are then deleted in the MemTable

Flushing happens asynchronously in a separate task.
Only a single flush can occur at a given point in time.

### Compacting

Compaction merges SST files to reduce read amplification and reclaim space:

1. **Level monitoring**: Each level tracks its total size and triggers compaction when exceeding either `flush_level_file_limit` if in flush level, or `level_limit * 10^level_key` otherwise
2. **Source selection**: The highest priority (i.e. oldest) entries from the source level are selected
3. **Target merging**: Selected SSTs are merged with overlapping SSTs in the target level (level + 1)
4. **Sorted merge**: Entries are merged in sorted order, with newer entries (higher sequence numbers) taking precedence
5. **Tombstone cleanup**: Deleted entries (tombstones) are removed at the deepest level
6. **New SST creation**: Merged data is written to new disk tables in the target level
7. **Manifest update**: Old files are marked for deletion and new files are logged
8. **File cleanup**: Old SST files are deleted

Compaction runs asynchronously.
Data is compressed for levels larger than 1.

## Disk table format

SST files use the `<no>.goblin` format and follow this binary structure:

| SST | SEPARATOR | FOOTER |
| --- | --- | --- |
| n * 512 bytes | 16 bytes | variable size |


### Sorted String Table (SST)

Each block is a multiple of 512 bytes and contains:
- **Block ID** (16 bytes): `"GOBLINBLOCK00000"`
- **Span** (2 bytes): Number of blocks this entry spans
- **Data** (variable): Erlang term encoding of `{sequence, key, value}`
- **Padding** (variable): Zero-filled to reach n * 512 bytes

### Footer

The footer contains metadata for efficient lookups:
- **Separator** (16 bytes): `"GOBLINSEP0000000"`
- **Bloom filter** (variable): Encoded bloom filter for membership testing
- **Key range** (variable): Min and max keys in the SST
- **Sequence range** (variable): Lowest and highest sequence numbers
- **Metadata** (57 bytes): Metadata info and positions and sizes of footer components
- **CRC** (32-bits): Checksum over SST, Bloom filter, key range, sequence range and metadata
- **Magic** (16 bytes): `"GOBLINFILE000000"`

The metadata section stores:
- Level key (1 bytes)
- Bloom filter position and size (16 bytes)
- Key range position and size (16 bytes)
- Sequence range position and size (16 bytes)
- Number of blocks (8 bytes)

## References

- [CubDB](https://github.com/lucaong/cubdb) - Embedded COW B+Tree database
- [RocksDB Documentation](https://github.com/facebook/rocksdb/wiki) - Facebook's LSM key-value store
