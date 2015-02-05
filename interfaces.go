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
	Batcher
}

type KeySpace interface {
	OneToOneTable(tableName, id string, row interface{}) OneToOneTable
	OneToManyTable(tableName, fieldToIndexBy, uniqueKey string, row interface{}) OneToManyTable
	TimeSeriesTable(tableName, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) TimeSeriesTable
	TimeSeriesBTable(tableName, fieldToIndexByField, timeField, uniqueKey string, bucketSize time.Duration, row interface{}) TimeSeriesBTable
	Table(tableName string, row interface{}, keys Keys) Table
	DebugMode(bool)
	Batcher
}

//
// OneToOne recipe
//

// Simple CRUD interface. You can do CRUD by Id.
type OneToOneTable interface {
	SetWithOptions(v interface{}, opts Options) error
	Set(v interface{}) error
	UpdateWithOptions(id interface{}, m map[string]interface{}, opts Options) error
	Update(id interface{}, m map[string]interface{}) error
	Delete(id interface{}) error
	Read(id, pointer interface{}) error
	MultiRead(ids []interface{}, pointerToASlice interface{}) error
	Batcher
	TableChanger
}

//
// OneToMany recipe
//

// OneToMany lets you list rows based on a field equality, eg. 'list all sales where seller id = v'.
type OneToManyTable interface {
	SetWithOptions(v interface{}, opts Options) error
	Set(v interface{}) error
	UpdateWithOptions(v, id interface{}, m map[string]interface{}, opts Options) error
	Update(v, id interface{}, m map[string]interface{}) error
	Delete(v, id interface{}) error
	DeleteAll(v interface{}) error
	List(v, startId interface{}, limit int, pointerToASlice interface{}) error
	Read(v, id, pointer interface{}) error
	MultiRead(id interface{}, ids []interface{}, pointerToASlice interface{}) error
	Batcher
	TableChanger
}

//
// TimeSeries recipe
//

// TimeSeries lets you list rows which have a field value between two date ranges.
type TimeSeriesTable interface {
	// timeField and idField must be present
	SetWithOptions(v interface{}, opts Options) error
	Set(v interface{}) error
	UpdateWithOptions(timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) error
	Update(timeStamp time.Time, id interface{}, m map[string]interface{}) error
	List(start, end time.Time, pointerToASlice interface{}) error
	Read(timeStamp time.Time, id, pointer interface{}) error
	Delete(timeStamp time.Time, id interface{}) error
	Batcher
	TableChanger
}

//
// TimeSeries B recipe
//

// TimeSeriesB is a cross between TimeSeries and OneToMany tables.
type TimeSeriesBTable interface {
	// timeField and idField must be present
	SetWithOptions(v interface{}, opts Options) error
	Set(v interface{}) error
	UpdateWithOptions(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) error
	Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) error
	List(v interface{}, start, end time.Time, pointerToASlice interface{}) error
	Read(v interface{}, timeStamp time.Time, id, pointer interface{}) error
	Delete(v interface{}, timeStamp time.Time, id interface{}) error
	Batcher
	TableChanger
}

//
// Raw CQL
//

// A Query is a subset of a Table intended to be read
type Query interface {
	Read(pointerToASlice interface{}) error
	ReadOne(pointer interface{}) error
	Limit(int) Query
}

// A Filter is a subset of a Table, filtered by Relations.
// You can do writes or reads on a filter.
type Filter interface {
	// Selection modifiers
	Query() Query
	// Partial update.
	Update(m map[string]interface{}) error // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	UpdateWithOptions(m map[string]interface{}, opts Options) error
	Delete() error
}

type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
}

type Batcher interface{
	Start()
	Commit() error
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
	Set(v interface{}) error
	SetWithOptions(v interface{}, opts Options) error
	Where(relations ...Relation) Filter // Because we provide selections
	TableChanger
}

type QueryExecutor interface {
	Query(stmt string, params ...interface{}) ([]map[string]interface{}, error)
	Execute(stmt string, params ...interface{}) error
}
