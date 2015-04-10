package gocassa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
)

const (
	read = iota
	singleRead
	delete
	update
	insert
)

type op struct {
	qe  QueryExecutor
	ops []singleOp
}

type singleOp struct {
	options Options
	f       filter
	opType  int
	result  interface{}
	m       map[string]interface{} // map for updates, sets etc
}

func newWriteOp(qe QueryExecutor, f filter, opType int, m map[string]interface{}) *op {
	return &op{
		qe: qe,
		ops: []singleOp{
			{
				f:      f,
				opType: opType,
				m:      m,
			},
		},
	}
}

func (w *singleOp) read(qe QueryExecutor, opt Options) error {
	stmt, params := w.generateRead(opt)
	maps, err := qe.Query(stmt, params...)
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(maps)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, w.result)
}

func (w *singleOp) readOne(qe QueryExecutor, opt Options) error {
	stmt, params := w.generateRead(opt)
	maps, err := qe.Query(stmt, params...)
	if err != nil {
		return err
	}
	if len(maps) == 0 {
		_, f, n, _ := runtime.Caller(3)
		return RowNotFoundError{
			file: f,
			line: n,
		}
	}
	bytes, err := json.Marshal(maps[0])
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, w.result)
}

func (w *singleOp) write(qe QueryExecutor, opt Options) error {
	stmt, params := w.generateWrite(opt)
	return qe.Execute(stmt, params...)
}

func (w *singleOp) run(qe QueryExecutor, opt Options) error {
	switch w.opType {
	case update, insert, delete:
		return w.write(qe, opt)
	case read:
		return w.read(qe, opt)
	case singleRead:
		return w.readOne(qe, opt)
	}
	return nil
}

func (w *op) Add(wo ...Op) Op {
	for _, v := range wo {
		w.ops = append(w.ops, v.(*op).ops...)
	}
	return w
}

func (w *op) Run() error {
	for _, v := range w.ops {
		err := v.run(w.qe, v.options)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *op) WithOptions(opt Options) Op {
	newOps := []singleOp{}
	for _, v := range w.ops {
		newOps = append(newOps, singleOp{
			options: v.options.Merge(opt),
			f:       v.f,
			opType:  v.opType,
			result:  v.result,
			m:       v.m,
		})
	}
	return &op{
		qe:  w.qe,
		ops: newOps,
	}
}

func (w op) RunAtomically() error {
	return fmt.Errorf("Not implemented yet")
	//stmts := []string{}
	//params := [][]interface{}{}
	//for _, vop := range w.ops {
	//	// We are pushing the limits of the type system here...
	//	if vop.opType == update || vop.opType == insert || vop.opType == delete {
	//		stmts = append(stmts, vop.stmt)
	//		params = append(params, vop.params)
	//	}
	//}
	//return w.qe.ExecuteAtomically(stmts, params)
}

func (o *singleOp) generateWrite(opt Options) (string, []interface{}) {
	var str string
	var vals []interface{}
	switch o.opType {
	case update:
		stmt, uvals := updateStatement(o.f.t.keySpace.name, o.f.t.Name(), o.m, o.f.t.options.Merge(opt))
		whereStmt, whereVals := generateWhere(o.f.rs)
		str = stmt + whereStmt
		vals = append(uvals, whereVals...)
	case delete:
		str, vals = generateWhere(o.f.rs)
		str = fmt.Sprintf("DELETE FROM %s.%s%s", o.f.t.keySpace.name, o.f.t.info.name, str)
	case insert:
		fields, insertVals := keyValues(o.m)
		str = insertStatement(o.f.t.keySpace.name, o.f.t.Name(), fields, o.f.t.options.Merge(opt))
		vals = insertVals
	}
	if o.f.t.keySpace.debugMode {
		fmt.Println(str, vals)
	}
	return str, vals
}

func (o *singleOp) generateRead(opt Options) (string, []interface{}) {
	w, wv := generateWhere(o.f.rs)
	ord, ov := o.generateOrderBy()
	lim, lv := o.generateLimit(o.f.t.options.Merge(opt))
	stmt := fmt.Sprintf("SELECT %s FROM %s.%s", o.f.t.generateFieldNames(), o.f.t.keySpace.name, o.f.t.Name())
	vals := []interface{}{}
	buf := new(bytes.Buffer)
	buf.WriteString(stmt)
	if w != "" {
		buf.WriteString(" ")
		buf.WriteString(w)
		vals = append(vals, wv...)
	}
	if ord != "" {
		buf.WriteString(" ")
		buf.WriteString(ord)
		vals = append(vals, ov...)
	}
	if lim != "" {
		buf.WriteString(" ")
		buf.WriteString(lim)
		vals = append(vals, lv...)
	}
	if o.f.t.keySpace.debugMode {
		fmt.Println(buf.String(), vals)
	}
	return buf.String(), vals
}

func (p *singleOp) generateOrderBy() (string, []interface{}) {
	return "", []interface{}{}
	// " ORDER BY %v"
}

func (o *singleOp) generateLimit(opt Options) (string, []interface{}) {
	if opt.Limit < 1 {
		return "", []interface{}{}
	}
	return "LIMIT ?", []interface{}{opt.Limit}
}

func generateWhere(rs []Relation) (string, []interface{}) {
	var (
		vals []interface{}
		buf  = new(bytes.Buffer)
	)

	if len(rs) > 0 {
		buf.WriteString(" WHERE ")
		for i, r := range rs {
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
		if mod, ok := v.(Modifier); ok {
			stmt, vals := mod.cql(k)
			buf.WriteString(stmt)
			ret = append(ret, vals...)
		} else {
			buf.WriteString(k + " = ?")
			ret = append(ret, v)
		}
		i++
	}

	return buf.String(), ret
}
