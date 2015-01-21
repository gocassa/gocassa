package gocassa

import (
	"fmt"
	"github.com/gocql/gocql"
)

type connection struct {
	s        *gocql.Session
	nodeIps  []string
	userName string
	pass     string
}

func Connect(nodeIps []string, username, password string) (Connection, error) {
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Consistency = gocql.One
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &connection{
		s:        sess,
		nodeIps:  nodeIps,
		userName: username,
		pass:     password,
	}, nil
}

func (c *connection) CreateKeySpace(name string) error {
	stmt := fmt.Sprintf("CREATE KEYSPACE %v WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", name)
	return c.s.Query(stmt).Exec()
}

func (c *connection) DropKeySpace(name string) error {
	stmt := fmt.Sprintf("DROP KEYSPACE IF EXISTS %v", name)
	return c.s.Query(stmt).Exec()
}
