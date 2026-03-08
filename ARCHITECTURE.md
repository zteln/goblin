# Architecture

Goblin is an embedded LSM-Tree (Log-Structured Merge-Tree) key-value database for Elixir. It runs inside your application's OTP supervision tree and provides ACID transactions, MVCC snapshot isolation, crash recovery via write-ahead logging, and automatic leveled compaction.

## Supervision Tree

```mermaid
graph TD
    G["Goblin (Supervisor)<br/>strategy: :rest_for_one"]
    R["Goblin.Registry<br/>(Elixir Registry)"]
    M["Goblin.Manifest<br/>(GenServer)"]
    B["Goblin.Broker<br/>(GenServer)"]
    D["Goblin.DiskTables<br/>(GenServer)"]
    MT["Goblin.MemTables<br/>(GenServer)"]

    G --> R
    G --> M
    G --> B
    G --> D
    G --> MT
```

## Core Components

```mermaid
graph LR
    API["Goblin API"]
    Broker["Broker"]
    MemTables["MemTables"]
    WAL["WAL"]
    DiskTables["DiskTables"]
    Manifest["Manifest"]

    API --> Broker
    Broker --> MemTables
    MemTables --> WAL
    MemTables --> DiskTables
    MemTables --> Manifest
    DiskTables --> Manifest
```

## Write Path

The complete data flow when writing (e.g. `Goblin.put(db, :alice, "Alice")`):

```mermaid
sequenceDiagram
    participant User
    participant Broker
    participant MT as MemTables
    participant WAL
    participant DT as DiskTables
    participant Manifest

    User->>Broker: put(db, :alice, "Alice")
    Broker->>Broker: serialize write, execute tx

    Broker->>MT: write(seq, writes)
    MT->>WAL: append + fsync
    MT->>Manifest: update seq
    MT->>MT: insert into ETS
    MT-->>Broker: :ok

    Broker-->>User: :ok

    Note over MT: If MemTable > mem_limit:
    MT->>DT: async flush to Level 0 SST
    DT->>Manifest: update disk tables
```

### Write Path Summary

1. **Serialize**: Only one writer at a time (others queue in the Broker)
2. **Durability**: WAL append + fsync **before** updating the MemTable
3. **In-memory update**: Insert into ETS ordered_set as `{key, -seq} → value`
4. **Async flush**: When MemTable exceeds `mem_limit`, flush to Level 0 SST in a background Task
5. **Cleanup**: Old MemTables are soft-deleted via SnapshotRegistry and garbage-collected once no snapshots reference them

## Read Path (Point Lookup)

The data flow when reading (e.g. `Goblin.get(db, :alice)`):

```mermaid
sequenceDiagram
    participant User
    participant Broker
    participant MT as MemTables
    participant DT as DiskTables

    User->>Broker: get(db, :alice)
    Broker->>Broker: snapshot active tables

    Broker->>MT: search MemTables (newest first)
    alt Found in memory
        MT-->>Broker: {key, value}
    else Not in memory
        Broker->>DT: search DiskTables level by level
        DT-->>Broker: {key, value} or :not_found
    end

    Broker-->>User: {:ok, "Alice"} or :not_found
```

### Read Path Summary

1. **Non-blocking**: Multiple readers run concurrently via MVCC snapshot isolation
2. **Level-by-level search**: MemTables (Level -1) → Level 0 → Level 1 → ... (newest data first)
3. **Early termination**: Once a key is found at a higher level, deeper levels are skipped for that key
4. **Bloom filters + key range checks** eliminate unnecessary disk I/O on DiskTables
5. **Binary search** within SST blocks for O(log n) point lookups

## Range Scan Path

The data flow for `Goblin.scan(db, min: :a, max: :z)`:

```mermaid
sequenceDiagram
    participant User
    participant Broker
    participant MT as MemTables
    participant DT as DiskTables

    User->>Broker: scan(db, min: :a, max: :z)
    Broker-->>User: lazy Stream

    Note over User: When stream is consumed:
    User->>MT: stream iterators for range
    User->>DT: stream iterators for range
    Note over User: K-way merge all iterators<br/>(deduplicate, filter tombstones)
    User-->>User: Stream of {key, value} pairs
```

## Compaction

```mermaid
flowchart TD
    L0["Level 0<br/>(flush target)"]
    L1["Level 1"]
    L2["Level 2+<br/>(compressed)"]
    Compact["K-Way Merge<br/>(async)"]

    L0 -->|file count trigger| Compact
    L1 -->|size trigger| Compact
    Compact -->|merged SSTs| L1
    Compact -->|merged SSTs| L2
```

### Compaction Rules

- **Level 0**: Triggers when file count >= `flush_level_file_limit` (default 4). All Level 0 files merge with overlapping Level 1 files.
- **Level 1+**: Triggers when total level size >= `level_base_size * level_size_multiplier ^ (level - 1)`. The file with the oldest sequence range merges with overlapping files in the next level.
- **Tombstone removal**: Tombstones are only stripped at the deepest level (no older versions can exist below).
- **Compression**: Levels >= 2 use `:erlang.term_to_binary/2` with the `:compressed` flag.
- Compaction runs as a `Task.async` and does not block reads or writes.

## Recovery on Startup

```mermaid
sequenceDiagram
    participant Manifest
    participant DiskTables
    participant MemTables
    participant WAL
    participant Broker

    Manifest->>Manifest: replay manifest log
    Manifest->>DiskTables: restore SST inventory
    Manifest->>MemTables: restore WAL inventory
    MemTables->>WAL: replay WAL files into ETS
    DiskTables->>Broker: ready
    MemTables->>Broker: ready
    Note over Broker: transactions now allowed
```
