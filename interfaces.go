package gocassa

import (
	"context"
	"time"
)

// Connection exists because one can not connect to a keyspace if it does not exist, thus having a Create on KeySpace is not possible.
// Use ConnectToKeySpace to acquire an instance of KeySpace without getting a Connection.
type Connection interface {
	CreateKeySpace(name string) error
	DropKeySpace(name string) error
	KeySpace(name string) KeySpace
}

// KeySpace is used to obtain tables from.
type KeySpace interface {
	// MapTable is a simple key-value store.
	MapTable(prefixForTableName, partitionKey string, rowDefinition interface{}) MapTable
	// MultimapTable lets you list rows based on a field equality, eg. 'list all sales where seller id = v'.
	// It uses the partitionKey for partitioning the data. The ordering within a partition is determined using the clusteringKey.
	MultimapTable(prefixForTableName, partitionKey, clusteringKey string, rowDefinition interface{}) MultimapTable
	// // MultimapMkTable lets you list rows based on several fields equality, eg. 'list all sales where seller id = v and name = 'john'.
	// It uses the partitionKey for partitioning the data. The ordering within a partition is determined by a set of clusteringKeys.
	MultimapMultiKeyTable(prefixForTableName string, partitionKeys, clusteringKeys []string, rowDefinition interface{}) MultimapMkTable
	/*
		TimeSeriesTable lets you list rows which have a field value between two date ranges.
		timeField is used as the partition key
		bucketSize is used to determine for what duration the data will be stored on the same partition.
	*/
	TimeSeriesTable(prefixForTableName, timeField, clusteringKey string, bucketSize time.Duration, rowDefinition interface{}) TimeSeriesTable
	MultiTimeSeriesTable(prefixForTableName, partitionKey, timeField, clusteringKey string, bucketSize time.Duration, rowDefinition interface{}) MultiTimeSeriesTable
	MultiKeyTimeSeriesTable(prefixForTableName string, partitionKeys []string, timeField string, clusteringKeys []string, bucketSize time.Duration, rowDefinition interface{}) MultiKeyTimeSeriesTable
	FlakeSeriesTable(prefixForTableName, idField string, bucketSize time.Duration, rowDefinition interface{}) FlakeSeriesTable
	MultiFlakeSeriesTable(prefixForTableName, indexField, idField string, bucketSize time.Duration, rowDefinition interface{}) MultiFlakeSeriesTable
	Table(prefixForTableName string, rowDefinition interface{}, keys Keys) Table
	// DebugMode enables/disables debug mode depending on the value of the input boolean.
	// When DebugMode is enabled, all built CQL statements are printe to stdout.
	DebugMode(bool)
	// Name returns the keyspace name as in C*
	Name() string
	// Tables returns the name of all configured column families in this keyspace
	Tables() ([]string, error)
	// Exists returns whether the specified column family exists within the keyspace
	Exists(string) (bool, error)
}

//
// Map recipe
//

// MapTable gives you basic CRUD functionality. If you need fancier ways to query your data set have a look at the other tables.
type MapTable interface {
	Set(value interface{}) Op
	Update(partitionKey interface{}, updatedValues map[string]interface{}) Op
	Delete(partitionKey interface{}) Op
	Read(partitionKey, pointer interface{}) Op
	MultiRead(partitionKeys []interface{}, pointerToASlice interface{}) Op
	WithOptions(Options) MapTable
	Table() Table
	TableChanger
}

//
// Multimap recipe
//

type MultimapTable interface {
	Set(value interface{}) Op
	Update(value, id interface{}, updatedValues map[string]interface{}) Op
	Delete(value, id interface{}) Op
	DeleteAll(value interface{}) Op
	List(partitionKey, clusteringKey interface{}, limit int, pointerToASlice interface{}) Op
	Read(partitionKey, clusteringKey, pointer interface{}) Op
	MultiRead(partitionKey interface{}, ids []interface{}, pointerToASlice interface{}) Op
	WithOptions(Options) MultimapTable
	Table() Table
	TableChanger
}

type MultimapMkTable interface {
	Set(v interface{}) Op
	Update(v, id map[string]interface{}, m map[string]interface{}) Op
	Delete(v, id map[string]interface{}) Op
	DeleteAll(v map[string]interface{}) Op
	List(v, startId map[string]interface{}, limit int, pointerToASlice interface{}) Op
	Read(v, id map[string]interface{}, pointer interface{}) Op
	MultiRead(v, id map[string]interface{}, pointerToASlice interface{}) Op
	WithOptions(Options) MultimapMkTable
	Table() Table
	TableChanger
}

//
// TimeSeries recipe
//

type TimeSeriesTable interface {
	// timeField and idField must be present
	Set(v interface{}) Op
	Update(timeStamp time.Time, id interface{}, m map[string]interface{}) Op
	Delete(timeStamp time.Time, id interface{}) Op
	Read(timeStamp time.Time, id, pointer interface{}) Op
	List(start, end time.Time, pointerToASlice interface{}) Op
	Buckets(start time.Time) Buckets
	WithOptions(Options) TimeSeriesTable
	Table() Table
	TableChanger
}

//
// TimeSeries B recipe
//

// MultiTimeSeriesTable is a cross between TimeSeries and Multimap tables.
type MultiTimeSeriesTable interface {
	// timeField and idField must be present
	Set(v interface{}) Op
	Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op
	Delete(v interface{}, timeStamp time.Time, id interface{}) Op
	Read(v interface{}, timeStamp time.Time, id, pointer interface{}) Op
	List(v interface{}, start, end time.Time, pointerToASlice interface{}) Op
	Buckets(v interface{}, start time.Time) Buckets
	WithOptions(Options) MultiTimeSeriesTable
	Table() Table
	TableChanger
}

// MultiKeyTimeSeriesTable is a cross between TimeSeries and MultimapMkTable tables.
type MultiKeyTimeSeriesTable interface {
	// timeField and idField must be present
	Set(v interface{}) Op
	Update(v map[string]interface{}, timeStamp time.Time, id map[string]interface{}, m map[string]interface{}) Op
	Delete(v map[string]interface{}, timeStamp time.Time, id map[string]interface{}) Op
	Read(v map[string]interface{}, timeStamp time.Time, id map[string]interface{}, pointer interface{}) Op
	List(v map[string]interface{}, start, end time.Time, pointerToASlice interface{}) Op
	Buckets(v map[string]interface{}, start time.Time) Buckets
	WithOptions(Options) MultiKeyTimeSeriesTable
	Table() Table
	TableChanger
}

type FlakeSeriesTable interface {
	Set(v interface{}) Op
	Update(id string, m map[string]interface{}) Op
	Delete(id string) Op
	Read(id string, pointer interface{}) Op
	List(start, end time.Time, pointerToASlice interface{}) Op
	Buckets(start time.Time) Buckets
	// ListSince queries the flakeSeries for the items after the specified ID but within the time window,
	// if the time window is zero then it lists up until 5 minutes in the future
	ListSince(id string, window time.Duration, pointerToASlice interface{}) Op
	WithOptions(Options) FlakeSeriesTable
	Table() Table
	TableChanger
}

type MultiFlakeSeriesTable interface {
	Set(v interface{}) Op
	Update(v interface{}, id string, m map[string]interface{}) Op
	Delete(v interface{}, id string) Op
	Read(v interface{}, id string, pointer interface{}) Op
	List(v interface{}, start, end time.Time, pointerToASlice interface{}) Op
	Buckets(v interface{}, start time.Time) Buckets
	// ListSince queries the flakeSeries for the items after the specified ID but within the time window,
	// if the time window is zero then it lists up until 5 minutes in the future
	ListSince(v interface{}, id string, window time.Duration, pointerToASlice interface{}) Op
	WithOptions(Options) MultiFlakeSeriesTable
	Table() Table
	TableChanger
}

//
// Raw CQL
//

// Filter is a subset of a Table, filtered by Relations.
// You can do writes or reads on a filter.
type Filter interface {
	// Update does a partial update. Use this if you don't want to overwrite your whole row, but you want to modify fields atomically.
	Update(m map[string]interface{}) Op // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	// Delete all rows matching the filter.
	Delete() Op
	// Reads all results. Make sure you pass in a pointer to a slice.
	Read(pointerToASlice interface{}) Op
	// ReadOne reads a single result. Make sure you pass in a pointer.
	ReadOne(pointer interface{}) Op
	// Table on which this filter operates.
	Table() Table
	// Relations which make up this filter. These should not be modified.
	Relations() []Relation
}

// Keys is used with the raw CQL Table type. It is implicit when using recipe tables.
type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
	Compound          bool //indicates if the partitions keys are gereated as compound key when no clustering columns are set
}

// Op is returned by both read and write methods, you have to run them explicitly to take effect.
// It represents one or more operations.
type Op interface {
	// Run the operation.
	Run() error
	// Run the operation, providing context to the executor.
	RunWithContext(context.Context) error
	// You do not need this in 95% of the use cases, use Run!
	// Using atomic batched writes (logged batches in Cassandra terminology) comes at a high performance cost!
	RunAtomically() error
	// Run the operation as an atomic (logged) batch, providing context to the executor.
	RunAtomicallyWithContext(context.Context) error
	// Add an other Op to this one.
	Add(...Op) Op
	// WithOptions lets you specify `Op` level `Options`.
	// The `Op` level Options and the `Table` level `Options` will be merged in a way that Op level takes precedence.
	// All queries in an `Op` will have the specified `Options`.
	// When using Add(), the existing options are preserved.
	// For example:
	//
	//    op1.WithOptions(Options{Limit:3}).Add(op2.WithOptions(Options{Limit:2})) // op1 has a limit of 3, op2 has a limit of 2
	//    op1.WithOptions(Options{Limit:3}).Add(op2).WithOptions(Options{Limit:2}) // op1 and op2 both have a limit of 2
	//
	WithOptions(Options) Op
	// Options lets you read the `Options` for this `Op`
	Options() Options
	// Preflight performs any pre-execution validation that confirms the op considers itself "valid".
	// NOTE: Run() and RunAtomically() should call this method before execution, and abort if any errors are returned.
	Preflight() error
	// GenerateStatement generates the statment and params to perform the operation
	GenerateStatement() (string, []interface{})
	// QueryExecutor returns the QueryExecutor
	QueryExecutor() QueryExecutor
}

// Danger zone! Do not use this interface unless you really know what you are doing
type TableChanger interface {
	// Create creates the table in the keySpace, but only if it does not exist already.
	// If the table already exists, it returns an error.
	Create() error
	// CreateStatement returns you the CQL query which can be used to create the table manually in cqlsh
	CreateStatement() (string, error)
	// Create creates the table in the keySpace, but only if it does not exist already.
	// If the table already exists, then nothing is created.
	CreateIfNotExist() error
	// CreateStatement returns you the CQL query which can be used to create the table manually in cqlsh
	CreateIfNotExistStatement() (string, error)
	// Recreate drops the table if exists and creates it again.
	// This is useful for test purposes only.
	Recreate() error
	// Name returns the name of the table, as in C*
	Name() string
}

// Table is the only non-recipe table, it is the "raw CQL table", it lets you do pretty much whatever you want
// with the downside that you have to know what you are doing - eg. you have to know what queries can you make
// on a certain partition key - clustering column combination.
type Table interface {
	// Set Inserts, or Replaces your row with the supplied struct. Be aware that what is not in your struct
	// will be deleted. To only overwrite some of the fields, use Query.Update.
	Set(v interface{}) Op
	// Where accepts a bunch of realtions and returns a filter. See the documentation for Relation and Filter to understand what that means.
	Where(relations ...Relation) Filter // Because we provide selections
	// Name returns the underlying table name, as stored in C*
	WithOptions(Options) Table
	TableChanger
}

// QueryExecutor actually executes the queries - this is mostly useful for testing/mocking purposes,
// ignore this otherwise. This library is using github.com/gocql/gocql as the query executor by default.
type QueryExecutor interface {
	// Query executes a query and returns the results.  It also takes Options to do things like set consistency
	QueryWithOptions(opts Options, stmt string, params ...interface{}) ([]map[string]interface{}, error)
	// Query executes a query and returns the results
	Query(stmt string, params ...interface{}) ([]map[string]interface{}, error)
	// Execute executes a DML query. It also takes Options to do things like set consistency
	ExecuteWithOptions(opts Options, stmt string, params ...interface{}) error
	// Execute executes a DML query
	Execute(stmt string, params ...interface{}) error
	// ExecuteAtomically executes multiple DML queries with a logged batch
	ExecuteAtomically(stmt []string, params [][]interface{}) error
	// ExecuteAtomically executes multiple DML queries with a logged batch, and takes options
	ExecuteAtomicallyWithOptions(opts Options, stmt []string, params [][]interface{}) error
}

type Counter int

// Buckets is an iterator over a timeseries' buckets
type Buckets interface {
	Filter() Filter
	Bucket() time.Time
	Next() Buckets
	Prev() Buckets
}
