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
func updateStatement(kn, cfName string, fields map[string]interface{}, opts Options) (string, []interface{}) {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("UPDATE %s.%s ", kn, cfName))

	// Apply options
	if opts.TTL != 0 {
		buf.WriteString("USING TTL ")
		buf.WriteString(strconv.FormatFloat(opts.TTL.Seconds(), 'f', 0, 64))
		buf.WriteRune(' ')
	}

	buf.WriteString("SET ")
	i := 0
	ret := []interface{}{}
	for k, v := range fields {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(k)
		buf.WriteString(" = ")
		if mod, ok := v.(Modifier); ok {
			stmt, vals := mod.cql()
			buf.WriteString(fmt.Sprintf(stmt, k))
			ret = append(ret, vals...)
		} else {
			buf.WriteString("?")
			ret = append(ret, v)
		}
		i++
	}

	return buf.String(), ret
}

func (f filter) UpdateWithOptions(m map[string]interface{}, opts Options) Op {
	str, wvals := f.generateWhere()
	stmt, uvals := updateStatement(f.t.keySpace.name, f.t.info.name, m, opts)
	vs := append(uvals, wvals...)
	if f.t.keySpace.debugMode {
		fmt.Println(stmt+" "+str, vs)
	}
	return newWriteOp(f.t.keySpace.qe, stmt+str, vs)
}

func (f filter) Update(m map[string]interface{}) Op {
	return f.UpdateWithOptions(m, Options{})
}

func (f filter) Delete() Op {
	str, vals := f.generateWhere()
	stmt := fmt.Sprintf("DELETE FROM %s.%s%s", f.t.keySpace.name, f.t.info.name, str)
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
