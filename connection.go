package gocassa

import (
	"fmt"
)

type connection struct {
	q QueryExecutor
}

// Convenience method to connect to
func Connect(nodeIps []string, username, password string) (Connection, error) {
	qe, err := newGoCQLBackend(nodeIps, username, password)
	if err != nil {
		return nil, err
	}
	return &connection{
		q: qe,
	}, nil
}

func NewConnection(q QueryExecutor) Connection {
	return &connection{
		q: q,
	}
}

func (c *connection) CreateKeySpace(name string) error {
	stmt := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", name)
	return c.q.Execute(stmt)
}

func (c *connection) DropKeySpace(name string) error {
	stmt := fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", name)
	return c.q.Execute(stmt)
}

func (c *connection) KeySpace(name string) KeySpace {
	return &k{
		qe:   c.q,
		name: name,
	}
}
