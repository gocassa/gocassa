package gocassa

import (
	"time"
)

// The Connection interface only exists because one can not connect to a keyspace if it does not exist, thus having a Create on KeySpace is not possible.
// Use ConnectToKeySpace to acquire an instance of KeySpace.
type Connection interface {
	CreateKeySpace(name string) error
	DropKeySpace(name string) error
	KeySpace(name string) KeySpace
}

type KeySpace interface {
	MapTable(tableName, id string, row interface{}) MapTable
	MultimapTable(tableName, fieldToIndexBy, uniqueKey string, row interface{}) MultimapTable
	TimeSeriesTable(tableName, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) TimeSeriesTable
	MultiTimeSeriesTable(tableName, fieldToIndexByField, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) MultiTimeSeriesTable
	Table(tableName string, row interface{}, keys Keys) Table
	DebugMode(bool)
	// QueryExecutor returns the configured executor for this KeySpace
	QueryExecutor() QueryExecutor
	// Name returns the name of the keyspace, as in C*
	Name() string
	// Tables returns the name of all configured column families in this keyspace
	Tables() ([]string, error)
	// Exists returns whether the specified column family exists within the keyspace
	Exists(string) (bool, error)
}

//
// Map recipe
//

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

// Multimap lets you list rows based on a field equality, eg. 'list all sales where seller id = v'.
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

// TimeSeries lets you list rows which have a field value between two date ranges.
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

// MultiTimeSeries is a cross between TimeSeries and Multimap tables.
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

// A Query is a subset of a Table intended to be read
type Query interface {
	Read(pointerToASlice interface{}) Op
	ReadOne(pointer interface{}) Op
	Limit(int) Query
}

// A Filter is a subset of a Table, filtered by Relations.
// You can do writes or reads on a filter.
type Filter interface {
	// Selection modifiers
	Query() Query
	// Partial update.
	Update(m map[string]interface{}) Op // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	UpdateWithOptions(m map[string]interface{}, opts Options) Op
	Delete() Op
}

type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
}

type Op interface {
	Run() error
	// You do not need this in 95% of the use cases, use Run!
	// Using atmoic batched writes (logged batches in Cassandra terminolohu) comes at a high performance cost!
	RunAtomically() error
	Add(...Op) Op
}

// Danger zone! Do not use this interface unless you really know what you are doing
type TableChanger interface {
	Create() error
	CreateStatement() (string, error)
	Recreate() error
	//Drop() error
	//CreateIfDoesNotExist() error
}

type Table interface {
	// Set Inserts, or Replaces your row with the supplied struct. Be aware that what is not in your struct
	// will be deleted. To only overwrite some of the fields, use Query.Update.
	Set(v interface{}) Op
	SetWithOptions(v interface{}, opts Options) Op
	Where(relations ...Relation) Filter // Because we provide selections
	// Name returns the underlying table name, as stored in C*
	Name() string
	TableChanger
}

type QueryExecutor interface {
	Query(stmt string, params ...interface{}) ([]map[string]interface{}, error)
	Execute(stmt string, params ...interface{}) error
	ExecuteAtomically(stmt []string, params [][]interface{}) error
}
