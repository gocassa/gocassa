# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

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
