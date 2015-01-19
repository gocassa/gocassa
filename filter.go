package cmagic

import (
	"strings"
	"fmt"
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

// UPDATE keyspace.Movies SET col1 = val1, col2 = val2
func updateStatement(cfName string, pkName string, fieldNames []string) string {
	cols := []string{}
	for _, v := range fieldNames {
		cols = append(cols, v+" = ?")
	}
	return fmt.Sprintf("UPDATE %v SET "+strings.Join(cols, ", "), cfName)
}

func (f filter) Update(m map[string]interface{}) error {
	fields, values := keyValues(m)
	for k, v := range m {
		fields = append(fields, k)
		values = append(values, v)
	}
	str, wvals := f.generateWhere()
	stmt := updateStatement(f.t.keySpace.name, f.t.info.name, fields)
	sess := f.t.keySpace.session
	if f.t.keySpace.debugMode {
		fmt.Println(stmt, wvals)
	}
	return sess.Query(stmt +" "+ str, append(values, wvals...)...).Exec()
}

func (f filter) Delete() error {
	str, vals := f.generateWhere()
	stmt := fmt.Sprintf("DELETE FROM %v.%v ", f.t.keySpace.name, f.t.info.name) + str
	if f.t.keySpace.debugMode {
		fmt.Println(stmt, vals)
	}
	return f.t.keySpace.session.Query(stmt, vals...).Exec()
}

func (f filter) Query() Query {
	return &query{
		f: f,
	}
}
