package cmagic

// This stuff is in flux.

// This is just an alias - unfortunately aliases in Go do not really work well -
// ie. you have to type cast to and from the original type.
type M map[string]interface{}

type KeySpace interface {
	Table(name string, row interface{}, keys Keys) Table
}

// A Query is a subset of a Table intended to be read
type Query interface {
	Read() ([]interface{}, error)
	Limit(int) Query
}

// A Filter is a subset of a Table, filtered by Relations.
// You can do operations or queries on a filter.
type Filter interface {
	// Selection modifiers
	Query() Query
	// Operations
	Update(m map[string]interface{}) error  // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	Delete() error
}

type Keys struct {
	PartitionKeys []string
	ClusteringColumns []string
}

type Table interface {
	// Set requires 
	Set(v interface{}) error 				
	Where(relations ...Relation) Filter 	// Because we provide selections
}
