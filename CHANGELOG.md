# Changelog

## v0.2.2 (2025-11-16)
* Enhancements
    * Added `Goblin.stop/3` function, allows one to stop the database supervisor
    * Added read-only transactions in `Goblin.transaction/2`
* Buf fixes
    * Fix registry name in `Goblin.is_flushing/1` and `Goblin.is_compacting/1`
    * Fix sequence counting bug when write-transaction commits

## v0.2.1 (2025-11-13)
* Enhancements
    * Add checksum verification to SST files
    * Data and metadata are compressed in SSTs for levels > 1

## v0.2.0 (2025-11-12)
* Enhancements
    * The transaction model is changed to true serial execution. 
    * The MemTable and Store utilize ETS tables to allow concurrent reads.
    * Bloom filter false positive probability is configurable
* Bug fixes
    * Change how hashes are stored in BloomFilter structs. Anonymous functions are no longer stored directly in the struct, instead the salt and range for the hash is stored instead. This prevents `BadFunctionError` if the module is updated.

## v0.1.5 (2025-11-02)
* Improvements
    * Improved SST filtering from store
* Enhancements
    * Add benchmarking scripts

## v0.1.4 (2025-11-01)
* Enhancements
    * Add registry for process discovery within the database server
    * Add registry for pubsub functionality within the database server
    * Add new supervisor for processes separate from registry processes
* Bug fixes
    * Provide name scopes to `WAL` and `Manifest`. This fixes the bug that multiple database servers couldn't start simultaneously.

## v0.1.3 (2025-10-29)
* Enhancements
    * Add `is_flushing/1` and `is_compacting/1` functions
    * Refactor `Reader.select/4` stream

## v0.1.2 (2025-10-29)
* Enhancements
    * Add `select/1/2` for range queries

## v0.1.1 (2025-10-27)
* Enhancements
    * Add `get_multi/2` functionality

## v0.1.0 (2025-10-26)
* Enhancements
    * First public release
