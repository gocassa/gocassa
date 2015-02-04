package gocassa

import (
	"fmt"
	"github.com/gocql/gocql"
)

type connection struct {
	q QueryExecutor
}

type goCQLBackend struct {
	session *gocql.Session
}

func (cb goCQLBackend) Query(stmt string, vals ...interface{}) ([]map[string]interface{}, error) {
	qu := cb.session.Query(stmt, vals...)
	iter := qu.Iter()
	ret := []map[string]interface{}{}
	m := &map[string]interface{}{}
	for iter.MapScan(*m) {
		ret = append(ret, *m)
		m = &map[string]interface{}{}
	}
	return ret, iter.Close()
}

func (cb goCQLBackend) Execute(stmt string, vals ...interface{}) error {
	return cb.session.Query(stmt, vals...).Exec()
}

func newGoCQLBackend(nodeIps []string, username, password string) (QueryExecutor, error) {
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
	return goCQLBackend{
		session: sess,
	}, nil
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
	k := &k{
		qe:   c.q,
		name: name,
	}
	k.tableFactory = k
	return k
}
