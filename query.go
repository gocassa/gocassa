package cmagic

import(
	"fmt"
)

// A Query is a subset of a Table intended to be read
type Query() interface {
	Read() (interface{}, error)
	ReadOne() ([]interface{}, error)
	Asc(bool) Query
	Options(QueryOptions) Query
	RowOptions(RowOptions)
}

type query struct {
	s *selection
}

func (q *Query) generateRead() string {
	fmt.Sprintf("SELECT * FROM %v WHERE %v ORDER BY %v, LIMIT %v", q.s.t.name, q.generateWhere(), q.generateOrderBy(), q.generateLimit())
}

func (q *Query) generateWhere() string {
	
}

func (q *Query) generateOrderBy() string {

}

func (q *Query) generateLimit() string {

}

