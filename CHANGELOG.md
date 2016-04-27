# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## v1.3.0 - 2016-04-26

### Added
 - New factory method, FlexMultiTimeSeriesTable, to create a MultiTimeSeriesTable with multiple index-fields and a supplied Bucketer.
   The existing factory method on KeySpace, MultiTimeSeriesTable, operates as before using a default implementation of Bucketer that
   provides the original behaviour.
 - Checks on table names  in tests
 - Added ability to close connections

### Changed
 - MultiTimeSeriesTable so that it can be configured with multiple index-fields and with an instance of an implementation of a new
   Bucketer interface to allow for alternative bucketing strategies, and for more flexible indexes

## v1.2.0 - 2015-12-22

### Added
 - Implemented `ORDER BY` for read queries

### Changed
- Updated mock tables to use new method of decoding, now also supports embedded and non-key map types.

## v1.1.0 - 2015-11-27

### Fixed
 - Fixed incorrect ordering of results in `MockTable`
 - Fixed issue causing `Set` to fail with "PRIMARY KEY part user found in SET part" if keys are lower-case.

## v1.0.0 - 2015-11-13

### Added
 - Allow creating tables with compound keys
 - Added the `MultimapMultiKeyTable` recipe which allows for CRUD operations on rows filtered by equality of multiple fields.
 - Add support for compact storage and compression
 - Add `CreateIfNotExistStatement` and `CreateIfNotExist` functions to `Table`

### Changed
 - Improved how gocassa handles encoding+decoding, it no longer uses the `encoding/json` package and now supports embedded types and type aliases.
 - Added new functions to `QueryExecutor` interface (`QueryWithOptions` and `ExecuteWithOptions`)

### Fixed
 - Mock tables are now safe for concurrent use
 - `uint` types are now supported, when generating tables the cassandra `varint` type is used.
 - Fixed gocassa using `json` tags when decoding results into structs
