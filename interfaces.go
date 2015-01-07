package cmagic

// This stuff is in flux.

// This is just an alias - unfortunately aliases in Go do not really work well -
// ie. you have to type cast to and from the original type.
type M map[string]interface{}

type KeySpace interface {
	CustomTable(tableName string, row interface{}, keys Keys) Table
	EntityTable(tableName string)
	OneToManyTable(tableName, fieldToIndexBy, uniqueKey string) OneToManyTable
	TimeSeriesTable()
}

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

type Entity interface {
	
}

type OneToManyTable interface {
	Replace()
	List(v interface{})
	Update(v, v1, map[string]string{}) error
}

type TimeSeriesTable interface {

}

type Keys struct {
	PartitionKeys     []string
	ClusteringColumns []string
}

type CustomTable interface {
	// Set Inserts, or Replaces your row with the supplied struct. Be aware that what is not in your struct, will be deleted.
	// To only overwrite some of the fields, use Query.Update
	Set(v interface{}) error
	Where(relations ...Relation) Filter // Because we provide selections
}
