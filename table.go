package cmagic

import (
	"errors"
	r "github.com/hailocab/cmagic/reflect"
	"reflect"
	"fmt"
	"strings"
)

type table struct {
	keySpace   	*keySpace
	info 		*tableInfo
}

// Contains mostly analyzed information about the entity
type tableInfo struct {
	keyspace, name string
	entity         interface{}
	keys 		   Keys
	fieldNames     map[string]struct{} // This is here only to check containment
	fields         []string
	fieldValues    []interface{}
}

func newTableInfo(keyspace, name string, keys Keys, entity interface{}) *tableInfo {
	cinf := &tableInfo{
		keyspace:   keyspace,
		name:       name,
		entity:     entity,
		keys: 		keys,
	}
	fields, values, ok := r.FieldsAndValues(entity)
	if !ok {
		// panicking here since this is a programmer error
		panic("Supplied entity is not a struct")
	}
	cinf.fieldNames = map[string]struct{}{}
	for _, v := range fields {
		cinf.fieldNames[v] = struct{}{}
	}
	cinf.fields = fields
	cinf.fieldValues = values
	return cinf
}

func (t table) zero() interface{} {
	return reflect.New(reflect.TypeOf(t.info.entity)).Interface()
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
	case M:
		return map[string]interface{}(v), true
	case map[string]interface{}:
		return v, true
	}
	return r.StructToMap(i)
}

func (t table) Where(rs ...Relation) Filter {
	return filter{
		t: t,
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

func (c table) Set(i interface{}) error {
	m, ok := toMap(i)
	if !ok {
		return errors.New("Can't create: value not understood")
	}
	fields, values := keyValues(m)
	stmt := insert(c.info.name, fields)
	sess := c.keySpace.session
	return sess.Query(stmt, values...).Exec()
}
