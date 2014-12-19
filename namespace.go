package cmagic

import (
	"strings"

	"github.com/gocql/gocql"
)

type keySpace struct {
	session *gocql.Session
	name    string
	nodeIps []string
}

// New returns a new namespace. A namespace is analogous to keyspaces in Cassandra or databases in RDMSes.
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

// Collection returns a new Collection. A collection is analogous to column families in Cassandra or tables in RDBMSes.
func (n *keySpace) Collection(name string, entity interface{}) Table {
	return &collection{
		nameSpace:      n,
		collectionInfo: newCollectionInfo(n.name, name, "id", entity),
	}
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
