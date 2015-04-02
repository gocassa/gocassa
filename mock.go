package gocassa

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/gocql/gocql"
	"github.com/google/btree"
)

// MockKeySpace implements the KeySpace interface and constructs in-memory tables.
type mockKeySpace struct {
	k
}

type mockOp struct {
	funcs []func() error
}

func newOp(f func() error) mockOp {
	return mockOp{
		funcs: []func() error{f},
	}
}

func (m mockOp) Add(ops ...Op) Op {
	for _, o := range ops {
		m.funcs = append(m.funcs, o.(*mockOp).funcs...)
	}
	return m
}

func (m mockOp) Run() error {
	for _, f := range m.funcs {
		err := f()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mockOp) RunAtomically() error {
	return m.Run()
}

func (ks *mockKeySpace) NewTable(name string, entity interface{}, fields map[string]interface{}, keys Keys) Table {
	return &MockTable{
		name:   name,
		entity: entity,
		keys:   keys,
		rows:   map[rowKey]*btree.BTree{},
	}
}

func NewMockKeySpace() KeySpace {
	ks := &mockKeySpace{}
	ks.tableFactory = ks
	return ks
}

// MockTable implements the Table interface and stores rows in-memory.
type MockTable struct {
	// rows is mapping from row key to column group key to column map
	name    string
	rows    map[rowKey]*btree.BTree
	entity  interface{}
	keys    Keys
	options Options
}

type rowKey string
type superColumn struct {
	Key     *keyPart
	Columns map[string]interface{}
}

func (c *superColumn) Less(item btree.Item) bool {
	other, ok := item.(*superColumn)
	if !ok {
		return false
	}

	return c.Key.Less(other.Key)
}

type keyPart struct {
	Key   string
	Value interface{}
	Tail  *keyPart
}

func (k *keyPart) Prepend(key string, value interface{}) *keyPart {
	return &keyPart{Key: key, Value: value, Tail: k}
}

func (k *keyPart) Less(other *keyPart) bool {
	for part, other := k, other; part != nil && other != nil; part, other = part.Tail, other.Tail {
		cmp := bytes.Compare(part.Bytes(), other.Bytes())
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}

	return false
}

func (k *keyPart) Bytes() []byte {
	typeInfo := &gocql.TypeInfo{
		Proto: 0x03,
		Type:  cassaType(k.Value),
	}
	// FIXME handle err
	marshalled, _ := gocql.Marshal(typeInfo, k.Value)
	return marshalled
}

func (k *keyPart) RowKey() rowKey {
	buf := bytes.Buffer{}

	for part := k; part != nil; part = part.Tail {
		buf.Write(part.Bytes())
	}

	return rowKey(buf.String())
}

func (k *keyPart) ToSuperColumn() *superColumn {
	return &superColumn{Key: k}
}

func (t *MockTable) zero() interface{} {
	return reflect.New(reflect.TypeOf(t.entity)).Interface()
}

func (t *MockTable) keyFromColumnValues(columns map[string]interface{}, keys []string) (*keyPart, error) {
	var key *keyPart

	for _, column := range keys {
		value, ok := columns[column]

		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", column)
		}
		key = key.Prepend(column, value)
	}

	return key, nil
}

func (t *MockTable) Name() string {
	if len(t.options.TableName) > 0 {
		return t.options.TableName
	}
	return t.name
}

func (t *MockTable) getOrCreateRow(rowKey *keyPart) *btree.BTree {
	row := t.rows[rowKey.RowKey()]
	if row == nil {
		row = btree.New(2)
		t.rows[rowKey.RowKey()] = row
	}
	return row
}

func (t *MockTable) getOrCreateColumnGroup(rowKey, superColumnKey *keyPart) map[string]interface{} {
	row := t.getOrCreateRow(rowKey)
	scol := superColumnKey.ToSuperColumn()

	if row.Has(scol) {
		return row.Get(scol).(*superColumn).Columns
	}
	row.ReplaceOrInsert(scol)
	scol.Columns = map[string]interface{}{}

	return scol.Columns
}

func (t *MockTable) SetWithOptions(i interface{}, options Options) Op {
	return newOp(func() error {
		columns, ok := toMap(i)
		if !ok {
			return errors.New("Can't create: value not understood")
		}

		rowKey, err := t.keyFromColumnValues(columns, t.keys.PartitionKeys)
		if err != nil {
			return err
		}

		superColumnKey, err := t.keyFromColumnValues(columns, t.keys.ClusteringColumns)
		if err != nil {
			return err
		}

		superColumn := t.getOrCreateColumnGroup(rowKey, superColumnKey)

		for k, v := range columns {
			superColumn[k] = v
		}
		return nil
	})
}

func (t *MockTable) Set(i interface{}) Op {
	return t.SetWithOptions(i, t.options)
}

func (t *MockTable) Where(relations ...Relation) Filter {
	return &MockFilter{
		table:     t,
		relations: relations,
	}
}

func (t *MockTable) Create() error {
	return nil
}

func (t *MockTable) CreateStatement() (string, error) {
	return "", nil
}

func (t *MockTable) Recreate() error {
	return nil
}

func (t *MockTable) WithOptions(o Options) Table {
	return &MockTable{
		name:    t.name,
		rows:    t.rows,
		entity:  t.entity,
		keys:    t.keys,
		options: t.options.Merge(o),
	}
}

// MockFilter implements the Filter interface and works with MockTable.
type MockFilter struct {
	table     *MockTable
	relations []Relation
}

func (f *MockFilter) Query() Query {
	return &MockQuery{
		filter: f,
	}
}

func (f *MockFilter) rowMatch(row map[string]interface{}) bool {
	for _, relation := range f.relations {
		value := row[relation.key]
		if !relation.accept(value) {
			return false
		}
	}
	return true
}

func (f *MockFilter) keyRelationMap() map[string]Relation {
	result := map[string]Relation{}

	for _, relation := range f.relations {
		result[relation.key] = relation
	}

	return result
}

func (f *MockFilter) keysFromRelations(keys []string) ([]*keyPart, error) {
	keyRelationMap := f.keyRelationMap()
	var rowKey *keyPart
	var result []*keyPart

	if len(keys) == 0 {
		return []*keyPart{nil}, nil
	}

	for i, key := range keys {
		lastKey := i == len(keys)-1
		relation, ok := keyRelationMap[key]

		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", key)
		}

		if relation.op != equality && !(lastKey && relation.op == in) {
			return nil, fmt.Errorf("Invalid use of PK %s", key)
		}

		if !lastKey {
			rowKey = rowKey.Prepend(key, relation.terms[0])
		} else {
			for _, term := range relation.terms {
				result = append(result, rowKey.Prepend(relation.key, term))
			}
		}
	}

	return result, nil
}

func (f *MockFilter) UpdateWithOptions(m map[string]interface{}, options Options) Op {
	return newOp(func() error {
		rowKeys, err := f.keysFromRelations(f.table.keys.PartitionKeys)
		if err != nil {
			return err
		}

		for _, rowKey := range rowKeys {
			superColumnKeys, err := f.keysFromRelations(f.table.keys.ClusteringColumns)
			if err != nil {
				return err
			}

			for _, superColumnKey := range superColumnKeys {
				superColumn := f.table.getOrCreateColumnGroup(rowKey, superColumnKey)

				for _, keyPart := range []*keyPart{rowKey, superColumnKey} {
					for k := keyPart; k != nil; k = k.Tail {
						superColumn[k.Key] = k.Value
					}
				}

				for key, value := range m {
					superColumn[key] = value
				}
			}
		}

		return nil
	})
}

func (f *MockFilter) Update(m map[string]interface{}) Op {
	return f.UpdateWithOptions(m, Options{})
}

func (f *MockFilter) Delete() Op {
	return newOp(func() error {
		rowKeys, err := f.keysFromRelations(f.table.keys.PartitionKeys)
		if err != nil {
			return err
		}

		for _, rowKey := range rowKeys {
			row := f.table.rows[rowKey.RowKey()]
			if row == nil {
				return nil
			}

			row.Ascend(func(item btree.Item) bool {
				columns := item.(*superColumn).Columns
				if f.rowMatch(columns) {
					row.Delete(item)
				}

				return true
			})
		}

		return nil
	})
}

// MockQuery implements the Query interface and works with MockFilter.
type MockQuery struct {
	filter  *MockFilter
	options Options
}

func (q *MockQuery) Read(out interface{}) Op {
	return newOp(func() error {
		rowKeys, err := q.filter.keysFromRelations(q.filter.table.keys.PartitionKeys)
		if err != nil {
			return err
		}

		var result []map[string]interface{}
		for _, rowKey := range rowKeys {
			row := q.filter.table.rows[rowKey.RowKey()]
			if row == nil {
				continue
			}

			row.Ascend(func(item btree.Item) bool {
				columns := item.(*superColumn).Columns
				if q.filter.rowMatch(columns) {
					result = append(result, columns)
				}

				return true
			})
		}

		if q.options.Limit > 0 && q.options.Limit < len(result) {
			result = result[:q.options.Limit]
		}

		return q.assignResult(result, out)
	})
}

func (q *MockQuery) assignResult(records interface{}, out interface{}) error {
	bytes, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, out)
}

func (q *MockQuery) ReadOne(out interface{}) Op {
	return newOp(func() error {
		slicePtrVal := reflect.New(reflect.SliceOf(reflect.ValueOf(out).Elem().Type()))

		err := q.Read(slicePtrVal.Interface()).Run()
		if err != nil {
			return err
		}

		sliceVal := slicePtrVal.Elem()
		if sliceVal.Len() < 1 {
			return RowNotFoundError{}
		}
		q.assignResult(sliceVal.Index(0).Interface(), out)
		return nil
	})
}

func (q *MockQuery) Limit(limit int) Query {
	q.options = q.options.Merge(Limit(limit))
	return q
}
