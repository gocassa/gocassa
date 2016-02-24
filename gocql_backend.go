package gocassa

import (
	"errors"

	"github.com/gocql/gocql"
)

type goCQLBackend struct {
	session *gocql.Session
}

func (cb goCQLBackend) QueryWithOptions(opts Options, stmt string, vals ...interface{}) ([]map[string]interface{}, error) {
	qu := cb.session.Query(stmt, vals...)
	if opts.Consistency != nil {
		qu = qu.Consistency(*opts.Consistency)
	}
	iter := qu.Iter()
	ret := []map[string]interface{}{}
	m := &map[string]interface{}{}
	for iter.MapScan(*m) {
		ret = append(ret, *m)
		m = &map[string]interface{}{}
	}
	return ret, iter.Close()
}

func (cb goCQLBackend) Query(stmt string, vals ...interface{}) ([]map[string]interface{}, error) {
	return cb.QueryWithOptions(Options{}, stmt, vals...)
}

func (cb goCQLBackend) ExecuteWithOptions(opts Options, stmt string, vals ...interface{}) error {
	qu := cb.session.Query(stmt, vals...)
	if opts.Consistency != nil {
		qu = qu.Consistency(*opts.Consistency)
	}
	return qu.Exec()
}

func (cb goCQLBackend) Execute(stmt string, vals ...interface{}) error {
	return cb.ExecuteWithOptions(Options{}, stmt, vals...)
}

func (cb goCQLBackend) ExecuteAtomically(stmts []string, vals [][]interface{}) error {
	if len(stmts) != len(vals) {
		return errors.New("executeBatched: stmts length != param length")
	}

	if len(stmts) == 0 {
		return nil
	}
	batch := cb.session.NewBatch(gocql.LoggedBatch)
	for i, _ := range stmts {
		batch.Query(stmts[i], vals[i]...)
	}
	return cb.session.ExecuteBatch(batch)
}

func (cb goCQLBackend) Close() {
	cb.session.Close()
}

// GoCQLSessionToQueryExecutor enables you to supply your own gocql session with your custom options
// Then you can use NewConnection to mint your own thing
// See #90 for more details
func GoCQLSessionToQueryExecutor(sess *gocql.Session) QueryExecutor {
	return goCQLBackend{
		session: sess,
	}
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
	return GoCQLSessionToQueryExecutor(sess), nil
}
