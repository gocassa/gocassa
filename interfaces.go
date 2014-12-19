package cmagic

// This stuff is in flux.

// This is just an alias - unfortunately aliases in Go do not really work well -
// ie. you have to type cast to and from the original type.
type M map[string]interface{}

type KeySpace interface {
	Table(name string, row interface{}, keys Keys) Collection
}

// A Query is a subset of a collection intended to be read
type Query() interface {
	Read() (interface{}, error)
	ReadOne() ([]interface{}, error)
	Asc(bool) Query
	Options(QueryOptions) Query
	RowOptions(RowOptions)
}

// A Selection is a subset of a collection, 1 or more rows
type Selection interface {
	// Selection modifiers
	Between(from, to) Selection
	GreaterThan(v ...interface{}) Selection
	LesserThan(v ...interface{}) Selection
	Keys([]interface) Selection
	Query() Query
	// Operations
	Update(m map[string]interface{}) error  // Probably this is danger zone (can't be implemented efficiently) on a selectuinb with more than 1 document
	Replace(v interface{}) 					// Replace doesn't make sense on a selection which result in more than 1 document
	Delete(id string) error
}

type Keys struct {
	PartitionKeys []string
	CompositeKeys []string
}

type Table interface {
	Insert(v interface{}) error // Insert needs no selection, but all keys (PartitionKeys and CompositeKeys must be present in the struct)
	Select(keys []interface{}) Selection
}

// RowOptions
// See comment aboove 'ReadOpt' method
type RowOptions struct {
	ColumnNames []string
	ColumnStart *string
	ColumnEnd   *string
}

func NewRowOptions() *RowOptions {
	return &RowOptions{
		ColumnNames: []string{},
	}
}

// Set column names to return
func (r *RowOptions) ColNames(ns []string) *RowOptions {
	r.ColumnNames = ns
	return r
}

// Set start of the column names to return
func (r *RowOptions) ColStart(start string) *RowOptions {
	r.ColumnStart = &start
	return r
}

// Set end of the column names to return
func (r *RowOptions) ColEnd(end string) *RowOptions {
	r.ColumnEnd = &end
	return r
}

type QueryOptions struct {
	StartRowId *string
	EndRowId   *string
	RowLimit   *int
}

func NewQueryOptions() *QueryOptions {
	return &QueryOptions{}
}

func (q *QueryOptions) Start(rowId string) *QueryOptions {
	q.StartRowId = &rowId
	return q
}

func (q *QueryOptions) End(rowId string) *QueryOptions {
	q.EndRowId = &rowId
	return q
}
