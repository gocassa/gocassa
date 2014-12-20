package cmagic

import(
	"strings"
)

type query struct {
	f filter
}

func (q *query) generateRead() (string, []interface{}) {
	w, wv := q.generateWhere()
	o, ov := q.generateOrderBy()
	l, lv := q.generateLimit()
	str := "SELECT * FROM %v"
	vals := []interface{}{q.f.t.info.name}
	if len(wv) > 0 {
		str += w
		vals = append(vals, wv...)
	}
	if len(ov) > 0 {
		str += o
		vals = append(vals, ov...)
	}
	if len(lv) > 0 {
		str += l
		vals = append(vals, lv...)
	}
	return str, vals
}

func (q *query) generateWhere() (string, []interface{}) {
	strs := []string{}
	vals := []interface{}{}
	for _, v := range q.f.rs {
		s, v := v.cql()
		strs = append(strs, s)
		vals = append(vals, v...)
	}
	return strings.Join(strs, " AND "), vals
}

func (q *query) generateOrderBy() (string, []interface{}) {
	return "", []interface{}{}
	// " ORDER BY %v"
}

func (q *query) generateLimit() (string, []interface{}) {
	return "", []interface{}{}
	// " LIMIT %v"
}
