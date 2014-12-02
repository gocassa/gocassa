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
	ReadOpt(id string, opts RowOptions) (interface{}, error)
}

// I don't think this is needed at all, since we won't handle index tables probably, only 'entity' ones
type RowOptions interface {
	ColumnNames([]string) RowOptions
	ColumnStart(string) RowOptions 
	ColumnEnd(string) RowOptions
}

type QueryOptions interface {
	Start(string) QueryOptions
	End(string) QueryOptions
	Limit(int) QueryOptions
}

// These are just here to not forget about them

type TimeSeriesIndex interface {
}

type EqualityIndex interface {
	Equals(key string, value interface{}, opts QueryOptions)
}

// type Invoice struct {
// 		Id string
// 		CustomerId string
// 		Price int
// }
// opts.ColumnNames("id", "name").QueryResponseLimit(500)
// invoices, err := invoices.Equals("CustomerId", "500", "", "", 0, [])