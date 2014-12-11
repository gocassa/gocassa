package cmagic

// This stuff is in flux.

// This is just an alias - unfortunately aliases in Go do not really work well -
// ie. you have to type cast to and from the original type.
type M map[string]interface{}

type NameSpace interface {
	Collection(name string, entity interface{}) Collection
}

type Collection interface {
	Read(id string) (interface{}, error) // Read(id string, v interface{})???, that is more conventional in Go land
	// Just have a set method? How would that play with CQL?
	Create(v interface{}) error
	Update(v interface{}) error
	Delete(id string) error
	//MultiRead(ids []string) ([]interface{}, error)
	//List(idStart, idEnd string, limit int) ([]interface{}, error)

	// ReadOpt, RowOptions and the QueryOptions and related things are highly experimental - even more so than this library itself
	// I am not convinced that ReadOpts is needed at all - we should not touch the 'index tables' - we should use interfaces TimeSeries and EqualityIndex
	ReadOpt(id string, opts *RowOptions) (interface{}, error)
}

type EqualityIndex interface {
	Equals(key string, value interface{}, opts *QueryOptions) ([]interface{}, error)
}

type TimeSeriesIndex interface {
	//
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
