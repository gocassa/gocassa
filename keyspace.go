package cmagic

import (
	"fmt"
	"github.com/gocql/gocql"
	"strings"
)

type K struct {
	session *gocql.Session
	name    string
	nodeIps []string
}

// New returns a new keySpace. A keySpace is analogous to keyspaces in Cassandra or databases in RDMSes.
func New(nameSp, username, password string, nodeIps []string) (KeySpace, error) {
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Keyspace = nameSp
	cluster.Consistency = gocql.One
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &K{
		session: sess,
		name:    nameSp,
		nodeIps: nodeIps,
	}, nil
}

// Table returns a new Table. A Table is analogous to column families in Cassandra or tables in RDBMSes.
func (k *K) Table(name string, entity interface{}, keys Keys) Table {
	ti := newTableInfo(k.name, name, keys, entity)
	return &T{
		keySpace: k,
		info:     ti,
	}
}

// Returns table names in a keyspace
func (n *K) Tables() ([]string, error) {
	stmt := fmt.Sprintf("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='%v'", n.name)
	iter := n.session.Query(stmt).Iter()
	ret := []string{}
	m := map[string]interface{}{}
	for iter.MapScan(m) {
		ret = append(ret, m["columnfamily_name"].(string))
		m = map[string]interface{}{} // This is needed... @cruft
	}
	return ret, iter.Close()
}

func (k K) Exists(cf string) (bool, error) {
	ts, err := k.Tables()
	if err != nil {
		return false, err
	}
	for _, v := range ts {
		if v == cf {
			return true, nil
		}
	}
	return false, nil
}

func (k K) Drop(cf string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %v.%v", k.name, cf)
	return k.session.Query(stmt).Exec()
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
