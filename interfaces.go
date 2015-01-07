package cmagic

type KeySpace interface {
	OneToOneTable(tableName, id string, row interface{}) OneToOneTable
	OneToManyTable(tableName, fieldToIndexBy, uniqueKey string, row interface{}) OneToManyTable
	// TimeSeriesTable()
	// Rename this to CQLTable or something
	Table(tableName string, row interface{}, keys Keys) Table
}

//
// OneToOne recipe
//

type OneToOneTable interface {
	Set(v interface{}) error
	Update(id string, m map[string]interface{}) error
	Delete(id string) error
}

//
// OneToMany recipe
//

type OneToManyTable interface {
	Set(v interface{}) error
	Update(v, id interface{}, m map[string]interface{}) error
	Query(v, startId interface{}, limit int) ([]interface{}, error)
	Read(v, id interface{}) (interface{}, error)
}

//
// TimeSeries recipe
//

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
