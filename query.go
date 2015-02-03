package gocassa

import (
	"encoding/json"
	"fmt"
)

type query struct {
	f     filter
	limit int
}

func (q *query) Limit(i int) Query {
	q.limit = i
	return q
}

func (q *query) Read() ([]interface{}, error) {
	stmt, vals := q.generateRead()
	maps, err := q.f.t.keySpace.qe.Query(stmt, vals...)
	if err != nil {
		return nil, err
	}
	ret := []interface{}{}
	for _, m := range maps {
		bytes, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		r := q.f.t.zero()
		err = json.Unmarshal(bytes, r)
		if err != nil {
			return nil, err
		}
		ret = append(ret, r)

	}
	return ret, nil
}

func (q *query) ReadInto(pointerToASlice interface{}) error {
	stmt, vals := q.generateRead()
	maps, err := q.f.t.keySpace.qe.Query(stmt, vals...)
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(maps)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, pointerToASlice)
}

func (q *query) generateRead() (string, []interface{}) {
	w, wv := q.f.generateWhere()
	o, ov := q.generateOrderBy()
	l, lv := q.generateLimit()
	str := fmt.Sprintf("SELECT %s FROM %s.%s", q.f.t.generateFieldNames(), q.f.t.keySpace.name, q.f.t.info.name)
	vals := []interface{}{}
	if len(w) > 0 {
		str += " " + w
		vals = append(vals, wv...)
	}
	if len(o) > 0 {
		str += " " + o
		vals = append(vals, ov...)
	}
	if len(l) > 0 {
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
	if q.limit < 1 {
		return "", []interface{}{}
	}
	return "LIMIT ?", []interface{}{q.limit}
}
