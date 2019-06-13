package gocassa

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	r "github.com/monzo/gocassa/reflect"
)

type t struct {
	keySpace *k
	info     *tableInfo
	options  Options
}

// Contains mostly analyzed information about the entity
type tableInfo struct {
	keyspace, name string
	marshalSource  interface{}
	fieldSource    map[string]interface{}
	keys           Keys
	fieldNames     map[string]struct{} // This is here only to check containment
	fields         []string
	fieldValues    []interface{}
}

func newTableInfo(keyspace, name string, keys Keys, entity interface{}, fieldSource map[string]interface{}) *tableInfo {
	cinf := &tableInfo{
		keyspace:      keyspace,
		name:          name,
		marshalSource: entity,
		keys:          keys,
		fieldSource:   fieldSource,
	}
	fields := make([]string, 0, len(fieldSource))
	values := make([]interface{}, 0, len(fieldSource))
	for _, k := range sortedKeys(fieldSource) {
		fields = append(fields, k)
		values = append(values, fieldSource[k])
	}
	cinf.fieldNames = map[string]struct{}{}
	for _, v := range fields {
		cinf.fieldNames[v] = struct{}{}
	}
	cinf.fields = fields
	cinf.fieldValues = values
	return cinf
}

// Since we cant have Map -> [(k, v)] we settle for Map -> ([k], [v])
// #tuplelessLifeSucks
func keyValues(m map[string]interface{}) ([]string, []interface{}) {
	keys := []string{}
	values := []interface{}{}
	for _, k := range sortedKeys(m) {
		keys = append(keys, k)
		values = append(values, m[k])
	}
	return keys, values
}

func toMap(i interface{}) (m map[string]interface{}, ok bool) {
	switch v := i.(type) {
	case map[string]interface{}:
		m, ok = v, true
	default:
		m, ok = r.StructToMap(i)
	}

	return
}

func (t t) Where(rs ...Relation) Filter {
	return filter{
		t:  t,
		rs: rs,
	}
}

func (t t) generateColumnFieldList(sel []string) []string {
	xs := make([]string, len(t.info.fields))
	if len(sel) > 0 {
		xs = sel
	} else {
		for i, v := range t.info.fields {
			xs[i] = strings.ToLower(v)
		}
	}
	return xs
}

func relations(keys Keys, m map[string]interface{}) []Relation {
	ret := []Relation{}
	for _, v := range append(keys.PartitionKeys, keys.ClusteringColumns...) {
		ret = append(ret, Eq(v, m[v]))
	}
	return ret
}

func removeFields(m map[string]interface{}, s []string) map[string]interface{} {
	keys := map[string]bool{}
	for _, v := range s {
		v = strings.ToLower(v)
		keys[v] = true
	}
	ret := map[string]interface{}{}
	for k, v := range m {
		k = strings.ToLower(k)
		if !keys[k] {
			ret[k] = v
		}
	}
	return ret
}

func transformFields(m map[string]interface{}) {
	for k, v := range m {
		switch t := v.(type) {
		case Counter:
			m[k] = CounterIncrement(int(t))
		}
	}
}

// INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
//   VALUES ('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6', 'johndoe')
//
// Gotcha: primkey must be first
func insertStatement(keySpaceName, cfName string, fields map[string]interface{}, opts Options) (string, []interface{}) {
	fieldNames, vals := keyValues(fields)

	placeHolders := make([]string, len(fieldNames))
	lowerFieldNames := make([]string, len(fieldNames))
	for i, v := range fieldNames {
		placeHolders[i] = "?"
		lowerFieldNames[i] = strings.ToLower(v)
	}

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		keySpaceName,
		cfName,
		strings.Join(lowerFieldNames, ", "),
		strings.Join(placeHolders, ", ")))

	// Apply options
	if opts.TTL != 0 {
		buf.WriteString(" USING TTL ? ")
		vals = append(vals, strconv.FormatFloat(opts.TTL.Seconds(), 'f', 0, 64))
	}

	return buf.String(), vals
}

func (t t) Set(i interface{}) Op {
	m, ok := toMap(i)
	if !ok {
		panic("SetWithOptions: Incompatible type")
	}
	ks := append(t.info.keys.PartitionKeys, t.info.keys.ClusteringColumns...)
	updFields := removeFields(m, ks)
	if len(updFields) == 0 {
		return newWriteOp(t.keySpace.qe, filter{
			t: t,
		}, insertOpType, m)
	}
	transformFields(updFields)
	rels := relations(t.info.keys, m)
	return newWriteOp(t.keySpace.qe, filter{
		t:  t,
		rs: rels,
	}, updateOpType, updFields)
}

func (t t) Create() error {
	if stmt, err := t.CreateStatement(); err != nil {
		return err
	} else {
		return t.keySpace.qe.Execute(stmt)
	}
}

func (t t) CreateIfNotExist() error {
	if stmt, err := t.CreateIfNotExistStatement(); err != nil {
		return err
	} else {
		return t.keySpace.qe.Execute(stmt)
	}
}

func (t t) Recreate() error {
	if ex, err := t.keySpace.Exists(t.Name()); ex && err == nil {
		if err := t.keySpace.DropTable(t.Name()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return t.Create()
}

func (t t) CreateStatement() (Statement, error) {
	return createTable(t.keySpace.name,
		t.Name(),
		t.info.keys.PartitionKeys,
		t.info.keys.ClusteringColumns,
		t.info.fields,
		t.info.fieldValues,
		t.options.ClusteringOrder,
		t.info.keys.Compound,
		t.options.CompactStorage,
		t.options.Compressor,
	)
}

func (t t) CreateIfNotExistStatement() (Statement, error) {
	return createTableIfNotExist(t.keySpace.name,
		t.Name(),
		t.info.keys.PartitionKeys,
		t.info.keys.ClusteringColumns,
		t.info.fields,
		t.info.fieldValues,
		t.options.ClusteringOrder,
		t.info.keys.Compound,
		t.options.CompactStorage,
		t.options.Compressor,
	)
}

func (t t) Name() string {
	if len(t.options.TableName) > 0 {
		return t.options.TableName
	}
	return t.info.name
}

func (table t) WithOptions(o Options) Table {
	return t{
		keySpace: table.keySpace,
		info:     table.info,
		options:  table.options.Merge(o),
	}
}
