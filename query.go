package gocassa

import (
	"fmt"
)

type query struct {
	f       filter
	options Options
}

func (q *query) Limit(i int) Query {
	q.options = q.options.Merge(Limit(i))
	return q
}

func (q *query) Read(pointerToASlice interface{}) Op {
	stmt, vals := q.generateRead()
	return &op{
		qe: q.f.t.keySpace.qe,
		ops: []singleOp{
			{
				opType: read,
				result: pointerToASlice,
				stmt:   stmt,
				params: vals,
			},
		},
	}
}

func (q *query) ReadOne(pointer interface{}) Op {
	stmt, vals := q.generateRead()
	return &op{
		qe: q.f.t.keySpace.qe,
		ops: []singleOp{
			{
				opType: singleRead,
				result: pointer,
				stmt:   stmt,
				params: vals,
			},
		},
	}
}

func (q *query) generateRead() (string, []interface{}) {
	w, wv := generateWhere(q.f.rs)
	o, ov := q.generateOrderBy()
	l, lv := q.generateLimit()
	str := fmt.Sprintf("SELECT %s FROM %s.%s", q.f.t.generateFieldNames(), q.f.t.keySpace.name, q.f.t.Name())
	vals := []interface{}{}
	if w != "" {
		str += " " + w
		vals = append(vals, wv...)
	}
	if o != "" {
		str += " " + o
		vals = append(vals, ov...)
	}
	if l != "" {
		str += " " + l
		vals = append(vals, lv...)
	}
	if q.f.t.keySpace.debugMode {
		fmt.Println(str, vals)
	}
	return str, vals
}

func (q *query) generateOrderBy() (string, []interface{}) {
	return "", []interface{}{}
	// " ORDER BY %v"
}

func (q *query) generateLimit() (string, []interface{}) {
	if q.options.Limit < 1 {
		return "", []interface{}{}
	}
	return "LIMIT ?", []interface{}{q.options.Limit}
}
