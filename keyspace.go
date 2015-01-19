package cmagic

import (
	"github.com/gocql/gocql"
	"strings"
	"time"
	"fmt"
)

type K struct {
	session *gocql.Session
	name    	string
	// nodeIps 	[]string
	debugMode 	bool
}

func ConnectToKeySpace(name string, nodeIps []string, username, password string) (KeySpace, error) {
	// clean up this duplication
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Keyspace = name
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
		name:    name,
		// nodeIps: nodeIps,
	}, nil
}

func (k *K) DebugMode(b bool) {
	k.debugMode = true
}

// Table returns a new Table. A Table is analogous to column families in Cassandra or tables in RDBMSes.
func (k *K) Table(name string, entity interface{}, keys Keys) Table {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return k.table(name, entity, m, keys)
}

func (k *K) table(name string, entity interface{}, fieldSource map[string]interface{}, keys Keys) Table {
	ti := newTableInfo(k.name, name, keys, entity, fieldSource)
	return &T{
		keySpace: k,
		info:     ti,
	}
}

func (k *K) OneToOneTable(name, id string, row interface{}) OneToOneTable {
	return &oneToOne{
		t: k.Table(name, row, Keys{
			PartitionKeys: []string{id},
		}),
		idField: id,
	}
}

func (k *K) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *K) OneToManyTable(name, fieldToIndexBy, id string, row interface{}) OneToManyTable {
	return &oneToMany{
		t: k.Table(name, row, Keys{
			PartitionKeys: []string{fieldToIndexBy},
			ClusteringColumns: []string{id},
		}),
		idField: id,
		fieldToIndexBy: fieldToIndexBy,
	}
}

func (k *K) TimeSeriesTable(name, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesTable{
		t: k.table(name, row, m, Keys{
			PartitionKeys: []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
		timeField: timeField,
		idField: idField,
		bucketSize: bucketSize,
	}
}

func (k *K) TimeSeriesBTable(name, indexField, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesBTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesBTable{
		t: k.table(name, row, m, Keys{
			PartitionKeys: []string{indexField, bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
		indexField: indexField,
		timeField: timeField,
		idField: idField,
		bucketSize: bucketSize,
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
		if strings.ToLower(v) == strings.ToLower(cf) {
			return true, nil
		}
	}
	return false, nil
}

func (k K) DropTable(cf string) error {
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
