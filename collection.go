package cmagic

import(
	r "github.com/hailocab/cmagic/reflect"
	g "github.com/hailocab/cmagic/generate"
	"reflect"
	"encoding/json"
	"errors"
)

type collection struct {
	nameSpace 		*nameSpace
	collectionInfo 	*collectionInfo
}

// Contains mostly analyzed information about the entity
type collectionInfo struct {
	keyspace, name string
	entity         interface{}
	primaryKey     string
	fieldNames     map[string]struct{} // This is here only to check containment
	fields         []string
	fieldValues    []interface{}
}

func newCollectionInfo(keyspace, name, primaryKey string, entity interface{}) *collectionInfo {
	cinf := &collectionInfo{
		keyspace:   keyspace,
		name:       name,
		entity:     entity,
		primaryKey: primaryKey,
	}
	fields, values, ok := r.FieldsAndValues(entity)
	if !ok {
		// panicking here since this is a programmer error
		panic("Supplied entity is not a struct")
	}
	cinf.fieldNames = map[string]struct{}{}
	for _, v := range fields {
		if v == cinf.primaryKey {
			continue
		}
		cinf.fieldNames[v] = struct{}{}
	}
	cinf.fields = fields
	cinf.fieldValues = values
	return cinf
}

func (c collection) zero() interface{} {
	return reflect.New(reflect.TypeOf(c.collectionInfo.entity)).Interface()
}

func kvs(v map[string]interface{}) ([]string, []interface{}) {
	keys := []string{}
	values := []interface{}{}
	for k, v := range v {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}

func keyValues(i interface{}) ([]string, []interface{}, bool) {
	switch v := i.(type) {
	case M:
		keys, values := kvs(v)
		return keys, values, true
	case map[string]interface{}:
		keys, values := kvs(v)
		return keys, values, true
	}
	return r.FieldsAndValues(i)
}

// Will return 'entity' struct what was supplied when initializing the collection
func (c collection) Read(id string) (interface{}, error) {
	stmt := g.ReadById(c.nameSpace.name, c.collectionInfo.primaryKey)
	m := map[string]interface{}{}
	sess := c.nameSpace.session
	sess.Query(stmt, id).Iter().MapScan(m)
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	ret := c.zero()
	err = json.Unmarshal(bytes, ret)
	return ret, err
}

func (c collection) Create(i interface{}) error {
	fields, values, ok := keyValues(i)
	if !ok {
		return errors.New("can't create: entity is not a struct")
	}
	stmt := g.Insert(c.collectionInfo.name, fields)
	sess := c.nameSpace.session
	return sess.Query(stmt, values...).Exec()
}

// Use structs for the time being, no maps please.
func (c collection) Update(i interface{}) error {
	var m map[string]interface{}
	var ok bool
	m, ok = r.StructToMap(i)
	id, ok := m[c.collectionInfo.primaryKey]
	if !ok {
		return errors.New("collection.Set: primary key not found")
	}
	fields := []string{}
	values := []interface{}{}
	for k, v := range m {
		if k == c.collectionInfo.primaryKey {
			continue
		}
		fields = append(fields, k)
		values = append(values, v)
	}
	stmt := g.UpdateById(c.nameSpace.name, c.collectionInfo.primaryKey, fields)
	sess := c.nameSpace.session
	return sess.Query(stmt, append(values, id)...).Exec()
}

func (c collection) Delete(id string) error {
	return c.nameSpace.session.Query(g.DeleteById(c.nameSpace.name, c.collectionInfo.primaryKey), id).Exec()
}