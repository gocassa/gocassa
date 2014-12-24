package cmagic

import (
	"strings"
	g "github.com/hailocab/cmagic/generate"
)

type filter struct {
	t  T
	rs []Relation
}

func (f *filter) generateWhere() (string, []interface{}) {
	strs := []string{}
	vals := []interface{}{}
	for _, r := range f.rs {
		s, v := r.cql()
		strs = append(strs, s)
		vals = append(vals, v...)
	}
	return "WHERE " + strings.Join(strs, " AND "), vals
}

func (f filter) Replace(i interface{}) error {
	return nil
}

func (f filter) Update(m map[string]interface{}) error {
	fields, values := keyValues(m)
	for k, v := range m {
		fields = append(fields, k)
		values = append(values, v)
	}
	str, wvals := f.generateWhere()
	stmt := g.Update(f.t.keySpace.name, f.t.info.name, fields)
	sess := f.t.keySpace.session
	return sess.Query(stmt +" "+ str, append(values, wvals...)...).Exec()
}

func (f filter) Delete() error {
	// return c.keySpace.session.Query(g.DeleteById(c.keySpace.name, c.info.primaryKey), id).Exec()
	return nil
}

func (f filter) Query() Query {
	return &query{
		f: f,
	}
}
