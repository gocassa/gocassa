package gocassa

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"context"

	"github.com/gocql/gocql"
	"github.com/google/btree"
)

// MockKeySpace implements the KeySpace interface and constructs in-memory tables.
type mockKeySpace struct {
	k
}

type mockOp struct {
	options      Options
	funcs        []func(mockOp) error
	preflightErr error
}

func newOp(f func(mockOp) error) mockOp {
	return mockOp{
		funcs: []func(mockOp) error{f},
	}
}

func (m mockOp) Add(ops ...Op) Op {
	return mockMultiOp{m}.Add(ops...)
}

func (m mockOp) Run() error {
	for _, f := range m.funcs {
		err := f(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mockOp) RunWithContext(ctx context.Context) error {
	return m.WithOptions(Options{Context: ctx}).Run()
}

func (m mockOp) Options() Options {
	return m.options
}

func (m mockOp) WithOptions(opt Options) Op {
	return mockOp{
		options: m.options.Merge(opt),
		funcs:   m.funcs,
	}
}

func (m mockOp) RunAtomically() error {
	return m.Run()
}

func (m mockOp) RunAtomicallyWithContext(ctx context.Context) error {
	return m.WithOptions(Options{Context: ctx}).Run()
}

func (m mockOp) GenerateStatement() (string, []interface{}) {
	return "", []interface{}{}
}

func (m mockOp) QueryExecutor() QueryExecutor {
	return nil
}

func (m mockOp) Preflight() error {
	return m.preflightErr
}

type mockMultiOp []Op

func (mo mockMultiOp) Run() error {
	if err := mo.Preflight(); err != nil {
		return err
	}
	for _, op := range mo {
		if err := op.Run(); err != nil {
			return err
		}
	}
	return nil
}

func (mo mockMultiOp) RunWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo mockMultiOp) RunAtomically() error {
	return mo.Run()
}

func (mo mockMultiOp) RunAtomicallyWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo mockMultiOp) GenerateStatement() (string, []interface{}) {
	return "", []interface{}{}
}

func (mo mockMultiOp) QueryExecutor() QueryExecutor {
	return nil
}

func (mo mockMultiOp) Add(inOps ...Op) Op {
	ops := make(mockMultiOp, 0, len(inOps))
	for _, op := range inOps {
		// If any multiOps were passed, flatten them out
		switch op := op.(type) {
		case mockMultiOp:
			ops = append(ops, op...)
		case mockOp:
			ops = append(ops, op)
		default:
			panic("can't Add non-mock ops to mockMultiOp")
		}
	}
	return append(mo, ops...)
}

func (mo mockMultiOp) Options() Options {
	var opts Options
	for _, op := range mo {
		opts = opts.Merge(op.Options())
	}
	return opts
}

func (mo mockMultiOp) WithOptions(opts Options) Op {
	result := make(mockMultiOp, len(mo))
	for i, op := range mo {
		result[i] = op.WithOptions(opts)
	}
	return result
}

func (mo mockMultiOp) Preflight() error {
	for _, op := range mo {
		if err := op.Preflight(); err != nil {
			return err
		}
	}
	return nil
}

func (ks *mockKeySpace) NewTable(name string, entity interface{}, fields map[string]interface{}, keys Keys) Table {
	return &MockTable{
		name:   name,
		entity: entity,
		keys:   keys,
		rows:   map[rowKey]*btree.BTree{},
		mtx:    &sync.RWMutex{},
	}
}

func NewMockKeySpace() KeySpace {
	ks := &mockKeySpace{}
	ks.tableFactory = ks
	return ks
}

// MockTable implements the Table interface and stores rows in-memory.
type MockTable struct {
	sync.RWMutex

	// rows is mapping from row key to column group key to column map
	mtx     *sync.RWMutex
	name    string
	rows    map[rowKey]*btree.BTree
	entity  interface{}
	keys    Keys
	options Options
}

type rowKey string
type superColumn struct {
	Key     key
	Columns map[string]interface{}
}

func (c *superColumn) Less(item btree.Item) bool {
	other, ok := item.(*superColumn)
	if !ok {
		return false
	}

	return c.Key.Less(other.Key)
}

type gocqlTypeInfo struct {
	proto byte
	typ   gocql.Type
}

func (t gocqlTypeInfo) New() interface{} {
	return &gocqlTypeInfo{t.proto, t.typ}
}

func (t gocqlTypeInfo) Type() gocql.Type {
	return t.typ
}

func (t gocqlTypeInfo) Version() byte {
	return t.proto
}

func (t gocqlTypeInfo) Custom() string {
	return ""
}

type keyPart struct {
	Key   string
	Value interface{}
}

func (k *keyPart) Bytes() []byte {
	typeInfo := &gocqlTypeInfo{
		proto: 0x03,
		typ:   cassaType(k.Value),
	}
	marshalled, err := gocql.Marshal(typeInfo, k.Value)
	if err != nil {
		panic(err)
	}
	return marshalled
}

type key []keyPart

func (k key) Less(other key) bool {
	for i := 0; i < len(k) && i < len(other); i++ {
		cmp := bytes.Compare(k[i].Bytes(), other[i].Bytes())
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}

	return false
}

func (k key) RowKey() rowKey {
	buf := bytes.Buffer{}

	for _, part := range k {
		buf.Write(part.Bytes())
	}

	return rowKey(buf.String())
}

func (k key) ToSuperColumn() *superColumn {
	return &superColumn{Key: k}
}

func (k key) Append(column string, value interface{}) key {
	newKey := make([]keyPart, len(k)+1)
	copy(newKey, k)
	newKey[len(k)] = keyPart{column, value}
	return newKey
}

func (t *MockTable) partitionKeyFromColumnValues(values map[string]interface{}, keyNames []string) (key, error) {
	var key key

	// For a single partition key of type string, check that it is not
	// empty, this is same as this error from a real C* cluster-
	// InvalidRequest: Error from server: code=2200 [Invalid query]
	// message="Key may not be empty"
	if len(keyNames) == 1 {
		value, ok := values[keyNames[0]]
		stringVal, isString := value.(string)
		if !ok || (isString && stringVal == "") {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", keyNames[0])
		}
		key = key.Append(keyNames[0], value)
		return key, nil
	}

	// Cassandra _does_ allow you to have a composite partition key in which
	// all the components can be empty
	for _, keyName := range keyNames {
		value, ok := values[keyName]
		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", keyName)
		}
		key = key.Append(keyName, value)
	}

	return key, nil
}

func (t *MockTable) clusteringKeyFromColumnValues(values map[string]interface{}, keyNames []string) (key, error) {
	var key key

	for _, keyName := range keyNames {
		value, ok := values[keyName]
		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", keyName)
		}
		key = key.Append(keyName, value)
	}

	return key, nil
}

func (t *MockTable) Name() string {
	if len(t.options.TableName) > 0 {
		return t.options.TableName
	}
	return t.name
}

func (t *MockTable) getOrCreateRow(rowKey key) *btree.BTree {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	row := t.rows[rowKey.RowKey()]
	if row == nil {
		row = btree.New(2)
		t.rows[rowKey.RowKey()] = row
	}
	return row
}

func (t *MockTable) getOrCreateColumnGroup(rowKey, superColumnKey key) map[string]interface{} {
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
	return newOp(func(m mockOp) error {
		t.Lock()
		defer t.Unlock()

		columns, ok := toMap(i)
		if !ok {
			return errors.New("Can't create: value not understood")
		}

		rowKey, err := t.partitionKeyFromColumnValues(columns, t.keys.PartitionKeys)
		if err != nil {
			return err
		}

		superColumnKey, err := t.clusteringKeyFromColumnValues(columns, t.keys.ClusteringColumns)
		if err != nil {
			return err
		}

		superColumn := t.getOrCreateColumnGroup(rowKey, superColumnKey)

		if err := assignRecords(columns, superColumn); err != nil {
			return err
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

func (t *MockTable) CreateIfNotExist() error {
	return nil
}

func (t *MockTable) CreateIfNotExistStatement() (string, error) {
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
		mtx:     t.mtx,
	}
}

// MockFilter implements the Filter interface and works with MockTable.
type MockFilter struct {
	table     *MockTable
	relations []Relation
}

func (f *MockFilter) Table() Table {
	return f.table
}

func (f *MockFilter) Relations() []Relation {
	return f.relations
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

func (f *MockFilter) keysFromRelations(keyNames []string) ([]key, error) {
	keyRelationMap := f.keyRelationMap()
	var rowKey key
	var result []key

	if len(keyNames) == 0 {
		return []key{key{}}, nil
	}

	for i, keyName := range keyNames {
		lastKey := i == len(keyNames)-1
		relation, ok := keyRelationMap[keyName]

		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part `%s`", keyName)
		}

		if relation.op != equality && !(lastKey && relation.op == in) {
			return nil, fmt.Errorf("Invalid use of PK `%s`", keyName)
		}

		if !lastKey {
			rowKey = rowKey.Append(keyName, relation.terms[0])
		} else {
			for _, term := range relation.terms {
				result = append(result, rowKey.Append(relation.key, term))
			}
		}
	}

	return result, nil
}

func (f *MockFilter) UpdateWithOptions(m map[string]interface{}, options Options) Op {
	return newOp(func(mock mockOp) error {
		f.table.Lock()
		defer f.table.Unlock()

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

				for _, key := range []key{rowKey, superColumnKey} {
					for _, keyPart := range key {
						superColumn[keyPart.Key] = keyPart.Value
					}
				}

				if err := assignRecords(m, superColumn); err != nil {
					return err
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
	return newOp(func(m mockOp) error {
		f.table.Lock()
		defer f.table.Unlock()

		rowKeys, err := f.keysFromRelations(f.table.keys.PartitionKeys)
		if err != nil {
			return err
		}

		f.table.mtx.Lock()
		defer f.table.mtx.Unlock()
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

func (q *MockFilter) Read(out interface{}) Op {
	return newOp(func(m mockOp) error {
		q.table.Lock()
		defer q.table.Unlock()

		var (
			result []map[string]interface{}
			err    error
		)

		switch {
		case len(q.Relations()) == 0:
			result = q.readAllRows()
		default:
			result, err = q.readSomeRows()
		}
		if err != nil {
			return err
		}

		opt := q.table.options.Merge(m.options)
		if opt.Limit > 0 && opt.Limit < len(result) {
			result = result[:opt.Limit]
		}

		return q.assignResult(result, out)
	})
}

func (q *MockFilter) readSomeRows() ([]map[string]interface{}, error) {
	q.table.mtx.RLock()
	defer q.table.mtx.RUnlock()

	rowKeys, err := q.keysFromRelations(q.table.keys.PartitionKeys)
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for _, rowKey := range rowKeys {
		row := q.table.rows[rowKey.RowKey()]
		if row == nil {
			continue
		}

		row.Ascend(func(item btree.Item) bool {
			columns := item.(*superColumn).Columns
			if q.rowMatch(columns) {
				result = append(result, columns)
			}

			return true
		})
	}

	return result, nil
}

func (q *MockFilter) readAllRows() []map[string]interface{} {
	q.table.mtx.RLock()
	defer q.table.mtx.RUnlock()
	var result []map[string]interface{}
	for _, row := range q.table.rows {
		row.Ascend(func(item btree.Item) bool {
			columns := item.(*superColumn).Columns
			if q.rowMatch(columns) {
				result = append(result, columns)
			}

			return true
		})
	}
	return result
}

func (q *MockFilter) assignResult(records interface{}, out interface{}) error {
	return decodeResult(records, out)
}

func (q *MockFilter) ReadOne(out interface{}) Op {
	return newOp(func(m mockOp) error {
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

func assignRecords(m map[string]interface{}, record map[string]interface{}) error {
	for k, v := range m {
		switch v := v.(type) {
		case Modifier:
			switch v.op {
			case modifierMapSetField:
				// Go interfaces are internally represented as a type and a value. The record[k] interface{} value could look like one of these:
				// [type, value]
				// [type, nil  ]
				// [nil,  nil  ]
				var targetMap reflect.Value
				if record[k] != nil {
					// narrowed it down to:
					// [type, value]
					// [type, nil  ]
					rv := reflect.ValueOf(record[k])

					if rv.Type().Kind() != reflect.Map {
						return fmt.Errorf("Can't use MapSetField modifier on field that isn't a map: %T", record[k])
					}

					if !rv.IsNil() {
						// [type, value]
						targetMap = rv
					}
				}

				// This modifier's args is a []interface{} with a key at index 0 and a value at index 1
				if len(v.args) != 2 {
					return fmt.Errorf("Argument for MapSetField is not a slice of 2 elements")
				}

				key := reflect.ValueOf(v.args[0])
				value := reflect.ValueOf(v.args[1])

				// If we couldn't initialize the map from the content of record[k], we create it from the values of v.args
				if targetMap.Kind() != reflect.Map {
					targetMapType := reflect.MapOf(key.Type(), value.Type())
					targetMap = reflect.MakeMap(targetMapType)
				}

				targetMap.SetMapIndex(key, value)

				record[k] = targetMap.Interface()
			case modifierMapSetFields:
				// Go interfaces are internally represented as a type and a value. The record[k] interface{} value could look like one of these:
				// [type, value]
				// [type, nil  ]
				// [nil,  nil  ]
				var targetMap reflect.Value
				if record[k] != nil {
					// narrowed it down to:
					// [type, value]
					// [type, nil  ]
					rv := reflect.ValueOf(record[k])

					if rv.Type().Kind() != reflect.Map {
						return fmt.Errorf("Can't use MapSetFields modifier on field that isn't a map: %T", record[k])
					}

					if rv.IsNil() {
						// [type, nil  ]
						targetMap = reflect.MakeMap(rv.Type())
					} else {
						// [type, value]
						targetMap = rv
					}
				} else {
					// [nil,  nil  ]
					// We don't know the type, so we guess. Note that this guess is
					// likely wrong but to fix that we need a much larger refactor.
					targetMap = reflect.ValueOf(map[string]interface{}{})
				}

				ma, ok := v.args[0].(map[string]interface{})
				if !ok {
					return fmt.Errorf("Argument for MapSetFields is not a map")
				}
				for k, v := range ma {
					targetMap.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
				}
				record[k] = targetMap.Interface()
			case modifierCounterIncrement:
				oldV, _ := record[k].(int64)
				delta := int64(v.args[0].(int))

				record[k] = oldV + delta
			default:
				return fmt.Errorf("Modifer %v not supported by mock keyspace", v.op)
			}
		default:
			record[k] = v
		}
	}

	return nil
}
