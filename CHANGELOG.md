# Changelog

## v0.1.5 ()
* Improvements
    * Filter SSTs in `Goblin.Store.get/2` by checking key range in directly in memory
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
