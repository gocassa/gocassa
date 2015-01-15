package cmagic

import (
	"time"
)

type KeySpace interface {
	OneToOneTable(tableName, id string, row interface{}) (OneToOneTable, error)
	OneToManyTable(tableName, fieldToIndexBy, uniqueKey string, row interface{}) (OneToManyTable, error)
	TimeSeriesTable(tableName, timeField, idField string, bucketSize time.Duration, row interface{}) (TimeSeriesTable, error)
	Table(tableName string, row interface{}, keys Keys) (Table, error)
}

//
// OneToOne recipe
//

type OneToOneTable interface {
	Set(v interface{}) error
	Update(id interface{}, m map[string]interface{}) error
	Delete(id interface{}) error
	Read(id interface{}) (interface{}, error)
	// MultiRead
}

//
// OneToMany recipe
//

// Maybe have UpdateAll and DeleteAll?
type OneToManyTable interface {
	Set(v interface{}) error
	Update(v, id interface{}, m map[string]interface{}) error
	Delete(v, id interface{}) error
	List(v, startId interface{}, limit int) ([]interface{}, error)
	Read(v, id interface{}) (interface{}, error)
	// MultiRead LATER
}

//
// TimeSeries recipe
//

// TimeSeries currently require both timestamp and UUID
// to identify a row, similarly to the OneToManyT recipe
type TimeSeriesTable interface {
	// timeField and idField must be present
	Set(v interface{}) error
	Update(timeStamp time.Time, id interface{}, m map[string]interface{}) error
	List(start, end time.Time) ([]interface{}, error)
	Read(timeStamp time.Time, id interface{}) (interface{}, error)
	Delete(timeStamp time.Time, id interface{}) error
}

//
// Raw CQL
//

// A Query is a subset of a Table intended to be read
type Query interface {
	Read() ([]interface{}, error)
	Limit(int) Query
	// For pagination
	// Start(token string) Query
}

// A Filter is a subset of a Table, filtered by Relations.
// You can do operations or queries on a filter.
type Filter interface {
	// Selection modifiers
	Query() Query
	// Partial update.
	Update(m map[string]interface{}) error // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	Delete() error
}

type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
}

type Table interface {
	// Set Inserts, or Replaces your row with the supplied struct. Be aware that what is not in your struct, will be deleted.
	// To only overwrite some of the fields, use Query.Update
	Set(v interface{}) error
	Where(relations ...Relation) Filter // Because we provide selections
}
