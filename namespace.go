package cmagic

import (
	r "github.com/hailocab/om/reflect"
	"strings"
	"github.com/hailocab/go-service-layer/cassandra"
	"github.com/hailocab/gossie/src/gossie"
)

type nameSpace struct {
	name 	string
}

// New returns a new namespace. A namespace is analogous to keyspaces in Cassandra or databases in RDMSes.
func New(name string) NameSpace {
	return &nameSpace{
		name: name,
	}
}

// Collection returns a new Collection. A collection is analogous to column families in Cassandra or tables in RDBMSes.
func (n *nameSpace) Collection(name string, entity) Collection {
	return &collection{
		nameSpace: n,
		collectionInfo: newCollectionInfo(n.name, name, "id", entity),
	}
}

type collection struct {
	nameSpace *nameSpace,
	collectionInfo *collectionInfo,
}

// This type tries to encapsulate most of the shared code between IOStore and MemStore
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
		panic("om.newStore: Supplied entity is not a struct")
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

// Translate errors returned by cassandra
// Most of these should be checked after the appropriate commands only
// or otherwise may give false positive results.
//
// PS: we shouldn't do this, really

// Example: Cannot add existing keyspace "randKeyspace01"
func isKeyspaceExistsError(s string) bool {
	return strings.Contains(s, "exist")
}

func isTableExistsError(s string) bool {
	return strings.Contains(s, "exist")
}

// unavailable
// unconfigured columnfamily updtest1
func isMissingKeyspaceOrTableError(s string) bool {
	return strings.Contains(s, "unavailable") || strings.Contains(s, "unconfigured")
}