package gocassa

import (
	"bytes"
	"fmt"
	"strconv"
)

type filter struct {
	t  t
	rs []Relation
}

func (f *filter) generateWhere() (string, []interface{}) {
	var (
		vals []interface{}
		buf  = new(bytes.Buffer)
	)

	if len(f.rs) > 0 {
		buf.WriteString(" WHERE ")
		for i, r := range f.rs {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			s, v := r.cql()
			buf.WriteString(s)
			vals = append(vals, v...)
		}
	}
	return buf.String(), vals
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

func (f filter) UpdateWithOptions(m map[string]interface{}, opts Options) Op {
	fields, values := keyValues(m)
	str, wvals := f.generateWhere()
	stmt := updateStatement(f.t.keySpace.name, f.t.info.name, fields, opts)
	if f.t.keySpace.debugMode {
		fmt.Println(stmt+" "+str, append(values, wvals...))
	}
	return newWriteOp(f.t.keySpace.qe, stmt+str, append(values, wvals...))
}

// Do a partial update on the filter.
func (f filter) Update(m map[string]interface{}) Op {
	return f.UpdateWithOptions(m, Options{})
}

// Delete all rows matching the filter.
func (f filter) Delete() Op {
	str, vals := f.generateWhere()
	stmt := fmt.Sprintf("DELETE FROM %s.%s%s", f.t.keySpace.name, f.t.info.name, str)
	if f.t.keySpace.debugMode {
		fmt.Println(stmt, vals)
	}
	return newWriteOp(f.t.keySpace.qe, stmt, vals)
}

// Return the query from the filter so you can read the rows matching the filter.
func (f filter) Query() Query {
	return &query{
		f: f,
	}
}
