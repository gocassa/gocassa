package gocassa

import (
	"github.com/gocql/gocql"
)

type goCQLBackend struct {
	session *gocql.Session
}

func (cb goCQLBackend) Query(stmt Statement, scanner Scanner) error {
	return cb.QueryWithOptions(Options{}, stmt, scanner)
}

func (cb goCQLBackend) QueryWithOptions(opts Options, stmt Statement, scanner Scanner) error {
	qu := cb.session.Query(stmt.Query(), stmt.Values()...)
	if opts.Consistency != nil {
		qu = qu.Consistency(*opts.Consistency)
	}

	iter := qu.Iter()
	_, err := scanner.ScanAll(iter)
	errClose := iter.Close()

	if err != nil {
		return err
	}

	return errClose
}

func (cb goCQLBackend) Execute(stmt Statement) error {
	return cb.ExecuteWithOptions(Options{}, stmt)
}

func (cb goCQLBackend) ExecuteWithOptions(opts Options, stmt Statement) error {
	qu := cb.session.Query(stmt.Query(), stmt.Values()...)
	if opts.Consistency != nil {
		qu = qu.Consistency(*opts.Consistency)
	}
	return qu.Exec()
}

func (cb goCQLBackend) ExecuteAtomically(stmts []Statement) error {
	return cb.ExecuteAtomicallyWithOptions(Options{}, stmts)
}

func (cb goCQLBackend) ExecuteAtomicallyWithOptions(opts Options, stmts []Statement) error {
	if len(stmts) == 0 {
		return nil
	}
	batch := cb.session.NewBatch(gocql.LoggedBatch)
	for i := range stmts {
		stmt := stmts[i]
		batch.Query(stmt.Query(), stmt.Values()...)
	}

	if opts.Consistency != nil {
		batch.Cons = *opts.Consistency
	}

	return cb.session.ExecuteBatch(batch)
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
