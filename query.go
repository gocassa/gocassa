package cmagic

import (
	"encoding/json"
	"fmt"
)

type query struct {
	f filter
	limit int
}

func (q *query) Limit(i int) Query {
	q.limit = i
	return q
}

func (q *query) Read() ([]interface{}, error) {
	stmt, vals := q.generateRead()
	sess := q.f.t.keySpace.session
	ret := []interface{}{}
	m := map[string]interface{}{}
	qu := sess.Query(stmt, vals...)
	iter := qu.Iter()
	for iter.MapScan(m) {
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
		m = map[string]interface{}{}
	}
	return ret, iter.Close()
}

func (q *query) generateRead() (string, []interface{}) {
	w, wv := q.f.generateWhere()
	o, ov := q.generateOrderBy()
	l, lv := q.generateLimit()
	str := fmt.Sprintf("SELECT * FROM %v.%v", q.f.t.keySpace.name, q.f.t.info.name)
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
