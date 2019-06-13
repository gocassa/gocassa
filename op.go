package gocassa

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"context"

	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	rreflect "github.com/monzo/gocassa/reflect"
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
	stmt := w.generateRead(w.options)
	maps, err := w.qe.QueryWithOptions(w.options, stmt)
	if err != nil {
		return err
	}

	return decodeResult(maps, w.result)
}

func (w *singleOp) readOne() error {
	stmt := w.generateRead(w.options)
	maps, err := w.qe.QueryWithOptions(w.options, stmt)
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
	fl := o.f.t.generateColumnFieldList(mopt.Select)
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

func decodeResult(m, result interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ZeroFields:       true,
		WeaklyTypedInput: true,
		Result:           result,
		TagName:          rreflect.TagName,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			decodeBigIntHook,
			decodeUnmarshalCQLHook,
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

// decodeUnmarshalCQLHook is hook function to decode custom type which
// implements gocql.Unmarshaler into the its custom type.
func decodeUnmarshalCQLHook(f, t reflect.Type, data interface{}) (interface{}, error) {
	// Only consider string ([]byte) → custom type
	if f == t || f.Kind() != reflect.String {
		return data, nil
	}

	var (
		rcvrType reflect.Type
		tIsValue bool
	)
	switch t.Kind() {
	case reflect.Ptr:
		rcvrType = t
	default:
		// Unmarshaling a non-pointer type is possible iff pointer
		// receiver of this type implements gocql.Unmarshaler
		rcvrType = reflect.PtrTo(t)
		tIsValue = true
	}

	unmarshaler := reflect.TypeOf((*gocql.Unmarshaler)(nil)).Elem()
	if rcvrType.Implements(unmarshaler) {
		// make sure obj is not nil by allocating
		obj := reflect.New(rcvrType.Elem()) // obj := new(*t)
		if !obj.CanInterface() || !obj.Elem().CanInterface() {
			// Give up if cannot type switch against Unmarshaler
			return data, nil
		}
		if u, ok := obj.Interface().(gocql.Unmarshaler); ok {
			var info gocql.TypeInfo // dummy info
			u.UnmarshalCQL(info, []byte(data.(string)))
			if tIsValue {
				// Convert this to non-reflect world
				return obj.Elem().Interface().(interface{}), nil // return *obj
			}
			return u, nil
		}
	}

	return data, nil
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
