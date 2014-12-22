package cmagic

import (
	"strings"
	"github.com/gocql/gocql"
	"fmt"
)

type keySpace struct {
	session *gocql.Session
	name    string
	nodeIps []string
}

// New returns a new keySpace. A keySpace is analogous to keyspaces in Cassandra or databases in RDMSes.
func New(nameSp, username, password string, nodeIps []string) (KeySpace, error) {
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Keyspace = nameSp
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &keySpace{
		session: sess,
		name:    nameSp,
		nodeIps: nodeIps,
	}, nil
}

// Table returns a new Table. A Table is analogous to column families in Cassandra or tables in RDBMSes.
func (n *keySpace) Table(name string, entity interface{}, keys Keys) Table {
	ti := newTableInfo(n.name, name, keys, entity)
	return &T{
		keySpace:   n,
		info: 		ti,
	}
}

// Returns table names in a keyspace
func (n *keySpace) Tables() ([]string, error) {
	stmt := fmt.Sprintf("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='%v'", n.name);
	iter := n.session.Query(stmt).Iter()
	ret := []string{}
	m := map[string]interface{}{}
	for iter.MapScan(m) {
		ret = append(ret, m["columnfamily_name"].(string))
		m = map[string]interface{}{} // This is needed... @cruft
	}
	return ret, iter.Close()
}

func (n keySpace) Exists(cf string) (bool, error) {
	ts, err := n.Tables()
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

func (n keySpace) Drop(cf string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %v.%v", n.name, cf);
	return n.session.Query(stmt).Exec()
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
