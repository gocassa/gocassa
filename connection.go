package gocassa

import (
	"fmt"
)

type connection struct {
	q QueryExecutor
}

// Connect to a cluster.
// If you are happy with default the options use this, if you need anything fancier, use `NewConnection`
func Connect(nodeIps []string, username, password string) (Connection, error) {
	qe, err := newGoCQLBackend(nodeIps, username, password)
	if err != nil {
		return nil, err
	}
	return &connection{
		q: qe,
	}, nil
}

// NewConnection creates a Connection with a custom query executor.
// Use `Connect` if you just want to talk to Cassandra with the default options.
// See `GoCQLSessionToQueryExecutor` if you want to use a gocql session with your own options as a `QueryExecutor`
func NewConnection(q QueryExecutor) Connection {
	return &connection{
		q: q,
	}
}

// CreateKeySpace creates a keyspace with the given name. Only used to create test keyspaces.
func (c *connection) CreateKeySpace(name string) error {
	stmt := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", name)
	return c.q.Execute(stmt)
}

// DropKeySpace drops the keyspace having the given name.
func (c *connection) DropKeySpace(name string) error {
	stmt := fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", name)
	return c.q.Execute(stmt)
}

// KeySpace returns the keyspace having the given name.
func (c *connection) KeySpace(name string) KeySpace {
	k := &k{
		qe:   c.q,
		name: name,
	}
	k.tableFactory = k
	return k
}

// Close closes the current session
// The connection should not be used again after calling Close()
func (c *connection) Close() {
	c.q.Close()
}
