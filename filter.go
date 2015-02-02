package gocassa

import (
	"bytes"
	"fmt"
	"strings"
)

type filter struct {
	t  t
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
func updateStatement(kn, cfName string, fieldNames []string, opts Options) string {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("UPDATE %s.%s SET ", kn, cfName))
	for i, v := range fieldNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(v)
		buf.WriteString(" = ?")
	}
	return buf.String()
}

func (f filter) UpdateWithOptions(m map[string]interface{}, opts Options) error {
	fields, values := keyValues(m)
	str, wvals := f.generateWhere()
	stmt := updateStatement(f.t.keySpace.name, f.t.info.name, fields, opts)
	sess := f.t.keySpace.session
	if f.t.keySpace.debugMode {
		fmt.Println(stmt+" "+str, append(values, wvals...))
	}
	return sess.Query(stmt+" "+str, append(values, wvals...)...).Exec()
}

func (f filter) Update(m map[string]interface{}) error {
	return f.UpdateWithOptions(m, Options{})
}

func (f filter) Delete() error {
	str, vals := f.generateWhere()
	stmt := fmt.Sprintf("DELETE FROM %s.%s ", f.t.keySpace.name, f.t.info.name) + str
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
