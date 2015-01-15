package cmagic

import (
	"errors"
	"fmt"
	g "github.com/hailocab/cmagic/generate"
	r "github.com/hailocab/cmagic/reflect"
	"reflect"
	"strings"
)

type T struct {
	keySpace *K
	info     *tableInfo
}

// Contains mostly analyzed information about the entity
type tableInfo struct {
	keyspace, name string
	marshalSource 	interface{}
	fieldSource   	map[string]interface{}
	keys           	Keys
	fieldNames     	map[string]struct{} // This is here only to check containment
	fields         	[]string
	fieldValues    	[]interface{}
}

func newTableInfo(keyspace, name string, keys Keys, entity interface{}, fieldSource map[string]interface{}) *tableInfo {
	cinf := &tableInfo{
		keyspace: 		keyspace,
		name:     		name,
		marshalSource:  entity,
		keys:     		keys,
		fieldSource: 	fieldSource,
	}
	fields := []string{}
	values := []interface{}{}
	for k, v := range fieldSource {
		fields = append(fields, k)
		values = append(values, v)
	}
	cinf.fieldNames = map[string]struct{}{}
	for _, v := range fields {
		cinf.fieldNames[v] = struct{}{}
	}
	cinf.fields = fields
	cinf.fieldValues = values
	return cinf
}

func (t T) zero() interface{} {
	return reflect.New(reflect.TypeOf(t.info.marshalSource)).Interface()
}

// Since we cant have Map -> [(k, v)] we settle for Map -> ([k], [v])
// #tuplelessLifeSucks
func keyValues(m map[string]interface{}) ([]string, []interface{}) {
	keys := []string{}
	values := []interface{}{}
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}

func toMap(i interface{}) (map[string]interface{}, bool) {
	switch v := i.(type) {
	//case M:
	//	return map[string]interface{}(v), true
	case map[string]interface{}:
		return v, true
	}
	return r.StructToMap(i)
}

func (t T) Where(rs ...Relation) Filter {
	return filter{
		t:  t,
		rs: rs,
	}
}

// INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
//   VALUES ('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6', 'johndoe')
//
// Gotcha: primkey must be first
func insert(cfName string, fieldNames []string) string {
	placeHolders := []string{}
	for i := 0; i < len(fieldNames); i++ {
		placeHolders = append(placeHolders, "?")
	}
	return fmt.Sprintf("INSERT INTO %v ("+strings.Join(fieldNames, ", ")+") VALUES ("+strings.Join(placeHolders, ", ")+")", cfName)
}

func (c T) Set(i interface{}) error {
	m, ok := toMap(i)
	if !ok {
		return errors.New("Can't create: value not understood")
	}
	fields, values := keyValues(m)
	stmt := insert(c.info.name, fields)
	sess := c.keySpace.session
	if c.keySpace.debugMode {
		fmt.Println(stmt, values)
	}
	return sess.Query(stmt, values...).Exec()
}

func (t T) Create() error {
	stmt, err := t.CreateStatement()
	if err != nil {
		return err
	}
	return t.keySpace.session.Query(stmt).Exec()
}

func (t T) CreateStatement() (string, error) {
	return g.CreateTable(t.keySpace.name, t.info.name, t.info.keys.PartitionKeys, t.info.keys.ClusteringColumns, t.info.fields, t.info.fieldValues)
}

//const (
//	asc	 = iota
//	desc
//)
//
//type Ordering struct {
//	fieldName string
//	order int
//}
//
//func ASC(fieldName string) Ordering {
//	return Ordering{
//		fieldName: fieldName,
//		order: asc,
//	}
//}
//
//func DESC(fieldName string) Ordering {
//	return Ordering{
//		fieldName: fieldName,
//		order: asc,
//	}
//}
