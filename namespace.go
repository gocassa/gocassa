package cmagic

import (
	"github.com/hailocab/go-service-layer/cassandra"
	"github.com/hailocab/gossie/src/gossie"
)

// Erdos represents a keyspace in cassa, or an index in ES (a similar analogy is a DataBase in RDMS terminology)
type Erdos struct {
	namespace          string
	//pluginConstructors map[string]func() Plugin // no plugins yet
}

// New returns a new namespace. A namespace is analogous to keyspaces in Cassandra or databases in RDMSes.
func New(namespace string) *Erdos {
	return &Erdos{
		namespace,
		map[string]func() Plugin{
			// Register your plugin here
			"eq":     NewEq,
			"search": NewSearch,
		},
	}
}

func (e *Erdos) getPool() (gossie.ConnectionPool, error) {
	return cassandra.ConnectionPool(e.namespace)
}

// Collection returns a new Collection. A collection is analogous to column families in Cassandra or tables in RDBMSes.
func (e *Erdos) Collection(name string) *Collection {
	return &Collection{
		e,
		name,
		map[string]interface{}{},
		map[string][]string{},
		"Id",
		true,
		map[string]Plugin{},
	}
}