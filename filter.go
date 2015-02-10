package gocassa

import (
	"bytes"
	"fmt"
	"strconv"
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

// UPDATE keyspace.Movies SET col1 = val1, col2 = val2
func updateStatement(kn, cfName string, fieldNames []string, opts Options) string {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("UPDATE %s.%s ", kn, cfName))

	// Apply options
	if opts.TTL != 0 {
		buf.WriteString("USING TTL ")
		buf.WriteString(strconv.FormatFloat(opts.TTL.Seconds(), 'f', 0, 64))
		buf.WriteRune(' ')
	}

	buf.WriteString("SET ")
	for i, v := range fieldNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(v)
		buf.WriteString(" = ?")
	}

	return buf.String()
}

func (f filter) UpdateWithOptions(m map[string]interface{}, opts Options) WriteOp {
	fields, values := keyValues(m)
	str, wvals := f.generateWhere()
	stmt := updateStatement(f.t.keySpace.name, f.t.info.name, fields, opts)
	if f.t.keySpace.debugMode {
		fmt.Println(stmt+" "+str, append(values, wvals...))
	}
	return newWriteOp(f.t.keySpace.qe, stmt+" "+str, append(values, wvals...))
}

func (f filter) Update(m map[string]interface{}) WriteOp {
	return f.UpdateWithOptions(m, Options{})
}

func (f filter) Delete() WriteOp {
	str, vals := f.generateWhere()
	stmt := fmt.Sprintf("DELETE FROM %s.%s ", f.t.keySpace.name, f.t.info.name) + str
	if f.t.keySpace.debugMode {
		fmt.Println(stmt, vals)
	}
	return newWriteOp(f.t.keySpace.qe, stmt, vals)
}

func (f filter) Query() Query {
	return &query{
		f: f,
	}
}
