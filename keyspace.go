package gocassa

import (
	"fmt"
	"github.com/gocql/gocql"
	"strings"
	"time"
)

type k struct {
	session *gocql.Session
	name    string
	// nodeIps 	[]string
	debugMode bool
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
	return &k{
		session: sess,
		name:    name,
		// nodeIps: nodeIps,
	}, nil
}

func (k *k) DebugMode(b bool) {
	k.debugMode = true
}

// Table returns a new Table. A Table is analogous to column families in Cassandra or tables in RDBMSes.
func (k *k) Table(name string, entity interface{}, keys Keys) Table {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return k.table(name, entity, m, keys)
}

func (k *k) table(name string, entity interface{}, fieldSource map[string]interface{}, keys Keys) Table {
	ti := newTableInfo(k.name, name, keys, entity, fieldSource)
	return &t{
		keySpace: k,
		info:     ti,
	}
}

func (k *k) OneToOneTable(name, id string, row interface{}) OneToOneTable {
	return &oneToOneT{
		t: k.Table(fmt.Sprintf("%s_oneToOne_%s", name, id), row, Keys{
			PartitionKeys: []string{id},
		}).(*t),
		idField: id,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) OneToManyTable(name, fieldToIndexBy, id string, row interface{}) OneToManyTable {
	return &oneToManyT{
		t: k.Table(fmt.Sprintf("%s_oneToMany_%s_%s", name, fieldToIndexBy, id), row, Keys{
			PartitionKeys:     []string{fieldToIndexBy},
			ClusteringColumns: []string{id},
		}).(*t),
		idField:        id,
		fieldToIndexBy: fieldToIndexBy,
	}
}

func (k *k) TimeSeriesTable(name, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesT{
		t: k.table(fmt.Sprintf("%s_timeSeries_%s_%s_%s", name, timeField, idField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}).(*t),
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

func (k *k) TimeSeriesBTable(name, indexField, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesBTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesBT{
		t: k.table(fmt.Sprintf("%s_timeSeries_%s_%s_%s_%s", name, indexField, timeField, idField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     []string{indexField, bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}).(*t),
		indexField: indexField,
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

// Returns table names in a keyspace
func (n *k) Tables() ([]string, error) {
	stmt := fmt.Sprintf("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='%s'", n.name)
	iter := n.session.Query(stmt).Iter()
	ret := []string{}
	m := map[string]interface{}{}
	for iter.MapScan(m) {
		ret = append(ret, m["columnfamily_name"].(string))
		m = map[string]interface{}{} // This is needed... @cruft
	}
	return ret, iter.Close()
}

func (k *k) Exists(cf string) (bool, error) {
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

func (k *k) DropTable(cf string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.name, cf)
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
