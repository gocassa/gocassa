package gocassa

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"context"
)

const (
	readOpType uint8 = iota
	singleReadOpType
	deleteOpType
	updateOpType
	insertOpType
)

type singleOp struct {
	options Options
	f       filter
	opType  uint8
	result  interface{}
	m       map[string]interface{} // map for updates, sets etc
	qe      QueryExecutor
}

func (o *singleOp) Options() Options {
	return o.options
}

func (o *singleOp) WithOptions(opts Options) Op {
	return &singleOp{
		options: o.options.Merge(opts),
		f:       o.f,
		opType:  o.opType,
		result:  o.result,
		m:       o.m,
		qe:      o.qe}
}

func (o *singleOp) Add(additions ...Op) Op {
	return multiOp{o}.Add(additions...)
}

func (o *singleOp) Preflight() error {
	return nil
}

func newWriteOp(qe QueryExecutor, f filter, opType uint8, m map[string]interface{}) *singleOp {
	return &singleOp{
		qe:     qe,
		f:      f,
		opType: opType,
		m:      m}
}

func (w *singleOp) read() error {
	stmt := w.generateRead(w.options).(statement)
	scanner := newScanner(stmt, w.result)
	err := w.qe.QueryWithOptions(w.options, stmt, scanner)
	if err != nil {
		return err
	}

	return nil
}

func (w *singleOp) readOne() error {
	stmt := w.generateRead(w.options).(statement)
	scanner := newScanner(stmt, w.result)
	err := w.qe.QueryWithOptions(w.options, stmt, scanner)
	if err != nil {
		return err
	}
	return nil
}

func (w *singleOp) write() error {
	stmt := w.generateWrite(w.options)
	return w.qe.ExecuteWithOptions(w.options, stmt)
}

func (o *singleOp) Run() error {
	switch o.opType {
	case updateOpType, insertOpType, deleteOpType:
		return o.write()
	case readOpType:
		return o.read()
	case singleReadOpType:
		return o.readOne()
	}
	return nil
}

func (o *singleOp) RunWithContext(ctx context.Context) error {
	return o.WithOptions(Options{Context: ctx}).Run()
}

func (o *singleOp) RunAtomically() error {
	return o.Run()
}

func (o *singleOp) RunAtomicallyWithContext(ctx context.Context) error {
	return o.WithOptions(Options{Context: ctx}).Run()
}

func (o *singleOp) GenerateStatement() Statement {
	switch o.opType {
	case updateOpType, insertOpType, deleteOpType:
		return o.generateWrite(o.options)
	case readOpType, singleReadOpType:
		return o.generateRead(o.options)
	}
	return noOpStatement
}

func (o *singleOp) QueryExecutor() QueryExecutor {
	return o.qe
}

func (o *singleOp) generateWrite(opt Options) Statement {
	var str string
	var vals []interface{}
	switch o.opType {
	case updateOpType:
		stmt, uvals := updateStatement(o.f.t.keySpace.name, o.f.t.Name(), o.m, o.f.t.options.Merge(opt))
		whereStmt, whereVals := generateWhere(o.f.rs)
		str = stmt + whereStmt
		vals = append(uvals, whereVals...)
	case deleteOpType:
		str, vals = generateWhere(o.f.rs)
		str = fmt.Sprintf("DELETE FROM %s.%s%s", o.f.t.keySpace.name, o.f.t.Name(), str)
	case insertOpType:
		str, vals = insertStatement(o.f.t.keySpace.name, o.f.t.Name(), o.m, o.f.t.options.Merge(opt))
	}
	if o.f.t.keySpace.debugMode {
		fmt.Println(str, vals)
	}
	return newStatement(str, vals)
}

func (o *singleOp) generateRead(opt Options) Statement {
	w, wv := generateWhere(o.f.rs)
	mopt := o.f.t.options.Merge(opt)
	fl := o.f.t.generateFieldList(mopt.Select)
	ord, ov := o.generateOrderBy(mopt)
	lim, lv := o.generateLimit(mopt)
	stmt := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(fl, ","), o.f.t.keySpace.name, o.f.t.Name())
	vals := []interface{}{}
	buf := new(bytes.Buffer)
	buf.WriteString(stmt)
	if w != "" {
		buf.WriteRune(' ')
		buf.WriteString(w)
		vals = append(vals, wv...)
	}
	if ord != "" {
		buf.WriteRune(' ')
		buf.WriteString(ord)
		vals = append(vals, ov...)
	}
	if lim != "" {
		buf.WriteRune(' ')
		buf.WriteString(lim)
		vals = append(vals, lv...)
	}
	if opt.AllowFiltering {
		buf.WriteString(" ")
		buf.WriteString("ALLOW FILTERING")
	}
	if o.f.t.keySpace.debugMode {
		fmt.Println(buf.String(), vals)
	}
	return newSelectStatement(buf.String(), vals, fl)
}

func (o *singleOp) generateOrderBy(opt Options) (string, []interface{}) {
	if len(opt.ClusteringOrder) < 1 {
		return "", []interface{}{}
	}

	buf := new(bytes.Buffer)
	buf.WriteString("ORDER BY ")
	for i, co := range opt.ClusteringOrder {
		if i > 0 {
			buf.WriteString(", ")
		}

		buf.WriteString(co.Column)
		buf.WriteString(" ")
		buf.WriteString(co.Direction.String())
	}
	return buf.String(), []interface{}{}
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
			if r.op == in {
				vals = append(vals, v)
				continue
			}
			vals = append(vals, v...)
		}
	}
	return buf.String(), vals
}

// UPDATE keyspace.Movies SET col1 = val1, col2 = val2
func updateStatement(kn, cfName string, fields map[string]interface{}, opts Options) (string, []interface{}) {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("UPDATE %s.%s ", kn, cfName))

	ret := []interface{}{}

	// Apply options
	if opts.TTL != 0 {
		buf.WriteString("USING TTL ? ")
		ret = append(ret, strconv.FormatFloat(opts.TTL.Seconds(), 'f', 0, 64))
	}

	buf.WriteString("SET ")
	i := 0
	for _, k := range sortedKeys(fields) {
		if i > 0 {
			buf.WriteString(", ")
		}
		if mod, ok := fields[k].(Modifier); ok {
			stmt, vals := mod.cql(k)
			buf.WriteString(stmt)
			ret = append(ret, vals...)
		} else {
			buf.WriteString(k + " = ?")
			ret = append(ret, fields[k])
		}
		i++
	}

	return buf.String(), ret
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
