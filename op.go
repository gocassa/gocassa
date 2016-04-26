package gocassa

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"runtime"
	"strconv"

	rreflect "github.com/hailocab/gocassa/reflect"
	"github.com/mitchellh/mapstructure"
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

// Used to pass errors back through the fluent API
type badOp struct {
	err error
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
	stmt, params := w.generateRead(w.options)
	maps, err := w.qe.QueryWithOptions(w.options, stmt, params...)
	if err != nil {
		return err
	}

	return decodeResult(maps, w.result)
}

func (w *singleOp) readOne() error {
	stmt, params := w.generateRead(w.options)
	maps, err := w.qe.QueryWithOptions(w.options, stmt, params...)
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
	return decodeResult(maps[0], w.result)
}

func (w *singleOp) write() error {
	stmt, params := w.generateWrite(w.options)
	return w.qe.ExecuteWithOptions(w.options, stmt, params...)
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

func (o *singleOp) RunAtomically() error {
	return o.Run()
}

func (o *singleOp) GenerateStatement() (string, []interface{}) {
	switch o.opType {
	case updateOpType, insertOpType, deleteOpType:
		return o.generateWrite(o.options)
	case readOpType, singleReadOpType:
		return o.generateRead(o.options)
	}
	return "", []interface{}{}
}

func (o *singleOp) QueryExecutor() QueryExecutor {
	return o.qe
}

//////

func (o *badOp) WithOptions(opts Options) Op {
	return o
}

func (o *badOp) Add(additions ...Op) Op {
	return o
}

func (o *badOp) Preflight() error {
	return o.err
}

func (o *badOp) Run() error {
	return o.err
}

func (o *badOp) RunAtomically() error {
	return o.Run()
}

func (o *badOp) GenerateStatement() (string, []interface{}) {
	return "", []interface{}{}
}

func (o *badOp) QueryExecutor() QueryExecutor {
	return nil
}

//////

func (o *singleOp) generateWrite(opt Options) (string, []interface{}) {
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
	mopt := o.f.t.options.Merge(opt)
	ord, ov := o.generateOrderBy(mopt)
	lim, lv := o.generateLimit(mopt)
	stmt := fmt.Sprintf("SELECT %s FROM %s.%s", o.f.t.generateFieldNames(mopt.Select), o.f.t.keySpace.name, o.f.t.Name())
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
	if opt.AllowFiltering {
		buf.WriteString(" ")
		buf.WriteString("ALLOW FILTERING")
	}
	if o.f.t.keySpace.debugMode {
		fmt.Println(buf.String(), vals)
	}
	return buf.String(), vals
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

func decodeResult(m, result interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ZeroFields:       true,
		WeaklyTypedInput: true,
		Result:           result,
		TagName:          rreflect.TagName,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			decodeBigIntHook,
		),
	})
	if err != nil {
		return err
	}

	return dec.Decode(m)
}

func decodeBigIntHook(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
	if f != reflect.Ptr {
		return data, nil
	}

	if i, ok := data.(*big.Int); ok {
		switch t {
		case reflect.Uint64:
			return i.Uint64(), nil
		case reflect.Uint32:
			return uint32(i.Uint64()), nil
		case reflect.Uint16:
			return uint16(i.Uint64()), nil
		case reflect.Uint8:
			return uint8(i.Uint64()), nil
		case reflect.Uint:
			return uint(i.Uint64()), nil
		case reflect.Int16:
			return int16(i.Int64()), nil
		case reflect.Int8:
			return int8(i.Int64()), nil
		}
	}

	return data, nil
}
