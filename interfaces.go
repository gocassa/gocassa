package gocassa

import (
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
	MapTable(tableName, id string, row interface{}) MapTable
	MultimapTable(tableName, fieldToIndexBy, uniqueKey string, row interface{}) MultimapTable
	TimeSeriesTable(tableName, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) TimeSeriesTable
	TimeSeriesTableWithName(tableName, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) TimeSeriesTable
	MultiTimeSeriesTable(tableName, fieldToIndexByField, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) MultiTimeSeriesTable
	MultiTimeSeriesTableWithName(tableName, fieldToIndexByField, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) MultiTimeSeriesTable
	Table(tableName string, row interface{}, keys Keys) Table
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
	SetWithOptions(v interface{}, opts Options) Op
	Set(v interface{}) Op
	UpdateWithOptions(id interface{}, m map[string]interface{}, opts Options) Op
	Update(id interface{}, m map[string]interface{}) Op
	Delete(id interface{}) Op
	Read(id, pointer interface{}) Op
	MultiRead(ids []interface{}, pointerToASlice interface{}) Op
	TableChanger
}

//
// Multimap recipe
//

// MultimapTable lets you list rows based on a field equality, eg. 'list all sales where seller id = v'.
type MultimapTable interface {
	SetWithOptions(v interface{}, opts Options) Op
	Set(v interface{}) Op
	UpdateWithOptions(v, id interface{}, m map[string]interface{}, opts Options) Op
	Update(v, id interface{}, m map[string]interface{}) Op
	Delete(v, id interface{}) Op
	DeleteAll(v interface{}) Op
	List(v, startId interface{}, limit int, pointerToASlice interface{}) Op
	Read(v, id, pointer interface{}) Op
	MultiRead(v interface{}, ids []interface{}, pointerToASlice interface{}) Op
	TableChanger
}

//
// TimeSeries recipe
//

// TimeSeriesTable lets you list rows which have a field value between two date ranges.
type TimeSeriesTable interface {
	// timeField and idField must be present
	SetWithOptions(v interface{}, opts Options) Op
	Set(v interface{}) Op
	UpdateWithOptions(timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) Op
	Update(timeStamp time.Time, id interface{}, m map[string]interface{}) Op
	Delete(timeStamp time.Time, id interface{}) Op
	Read(timeStamp time.Time, id, pointer interface{}) Op
	List(start, end time.Time, pointerToASlice interface{}) Op
	TableChanger
}

//
// TimeSeries B recipe
//

// MultiTimeSeriesTable is a cross between TimeSeries and Multimap tables.
type MultiTimeSeriesTable interface {
	// timeField and idField must be present
	SetWithOptions(v interface{}, opts Options) Op
	Set(v interface{}) Op
	UpdateWithOptions(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) Op
	Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op
	Delete(v interface{}, timeStamp time.Time, id interface{}) Op
	Read(v interface{}, timeStamp time.Time, id, pointer interface{}) Op
	List(v interface{}, start, end time.Time, pointerToASlice interface{}) Op
	TableChanger
}

//
// Raw CQL
//

// Query is a subset of a Table intended to be read
type Query interface {
	// Read the results. Make sure you pass in a pointer to a slice.
	Read(pointerToASlice interface{}) Op
	// Read one result. Make sure you pass in a pointer.
	ReadOne(pointer interface{}) Op
	// Limit the number of rows to be returned.
	Limit(int) Query
}

// Filter is a subset of a Table, filtered by Relations.
// You can do writes or reads on a filter.
type Filter interface {
	// Selection modifiers
	Query() Query
	// Updates does a partial update. Use this if you don't want to overwrite your whole row, but you want to modify fields atomically.
	Update(m map[string]interface{}) Op // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	// UpdateWithOptions is the same as Update but with extra options.
	UpdateWithOptions(m map[string]interface{}, opts Options) Op
	// Delete all rows matching the filter.
	Delete() Op
}

// Keys is used with the raw CQL Table type. It is implicit when using recipe tables.
type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
}

// Op is returned by both read and write methods, you have to run them explicitly to take effect.
// It represents one or more operations.
type Op interface {
	// Run the operation.
	Run() error
	// You do not need this in 95% of the use cases, use Run!
	// Using atomic batched writes (logged batches in Cassandra terminolohu) comes at a high performance cost!
	RunAtomically() error
	// Add an other Op to this one.
	Add(...Op) Op
}

// Danger zone! Do not use this interface unless you really know what you are doing
type TableChanger interface {
	// Create creates the table in the keySpace, but only if it does not exist already.
	// If the table already exists, it returns an error.
	Create() error
	// CreateStatement returns you the CQL query which can be used to create the tably manually in cqlsh
	CreateStatement() (string, error)
	// Recreate drops the table if exists and creates it again.
	// This is useful for test purposes only.
	Recreate() error
	// Name returns the name of the table, as in C*
	Name() string
	//Drop() error
	//CreateIfDoesNotExist() error
}

// Table is the only non-recipe table, it is the "raw CQL table", it lets you do pretty much whatever you want
// with the downside that you have to know what you are doing - eg. you have to know what queries can you make
// on a certain partition key - clustering column combination.
type Table interface {
	// Set Inserts, or Replaces your row with the supplied struct. Be aware that what is not in your struct
	// will be deleted. To only overwrite some of the fields, use Query.Update.
	Set(v interface{}) Op
	// SetWithOptions is the same as set, but with Options, like TTL, see the Options type for details
	SetWithOptions(v interface{}, opts Options) Op
	// Where accepts a bunch of realtions and returns a filter. See the documentation for Relation and Filter to understand what that means.
	Where(relations ...Relation) Filter // Because we provide selections
	// Name returns the underlying table name, as stored in C*
	TableChanger
}

// QueryExecutor actually executes the queries - this is mostly useful for testing/mocking purposes,
// ignore this otherwise. This library is using github.com/gocql/gocql as the query executor by default.
type QueryExecutor interface {
	// Query executes a query and returns the results
	Query(stmt string, params ...interface{}) ([]map[string]interface{}, error)
	// Execute executes a DML query
	Execute(stmt string, params ...interface{}) error
	// ExecuteAtomically executs multiple DML queries with a logged batch
	ExecuteAtomically(stmt []string, params [][]interface{}) error
}

type Counter int
