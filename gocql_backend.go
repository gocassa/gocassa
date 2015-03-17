package gocassa

import (
	"errors"

	"github.com/b2aio/gocql"
)

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

func (cb goCQLBackend) ExecuteAtomically(stmts []string, vals [][]interface{}) error {
	if len(stmts) != len(vals) {
		return errors.New("executeBatched: stmts length != param length")
	}
	// THIS IS A FAKE TEMPORAL IMPLEMENTATION
	for i, _ := range stmts {
		err := cb.Execute(stmts[i], vals[i])
		if err != nil {
			return err
		}
	}
	return nil
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
