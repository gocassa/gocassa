package gocassa

import (
	"fmt"
	"strings"
	"time"
)

type tableFactory interface {
	NewTable(string, interface{}, Keys) Table
}

type k struct {
	qe           QueryExecutor
	name         string
	debugMode    bool
	tableFactory tableFactory
}

func ConnectToKeySpace(keySpace string, nodeIps []string, username, password string) (KeySpace, error) {
	c, err := Connect(nodeIps, username, password)
	if err != nil {
		return nil, err
	}
	return c.KeySpace(keySpace), nil
}

func (k *k) DebugMode(b bool) {
	k.debugMode = true
}

// Table returns a new Table. A Table is analogous to column families in Cassandra or tables in RDBMSes.
func (k *k) Table(name string, entity interface{}, keys Keys) Table {
	return k.tableFactory.NewTable(name, entity, keys)
}

func (k *k) NewTable(name string, entity interface{}, keys Keys) Table {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}

	ti := newTableInfo(k.name, name, keys, m)
	return &t{
		keySpace: k,
		info:     ti,
	}
}

func (k *k) OneToOneTable(name, id string, row interface{}) OneToOneTable {
	return &oneToOneT{
		Table: k.Table(fmt.Sprintf("%s_oneToOne_%s", name, id), row, Keys{
			PartitionKeys: []string{id},
		}),
		idField: id,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) OneToManyTable(name, fieldToIndexBy, id string, row interface{}) OneToManyTable {
	return &oneToManyT{
		Table: k.Table(fmt.Sprintf("%s_oneToMany_%s_%s", name, fieldToIndexBy, id), row, Keys{
			PartitionKeys:     []string{fieldToIndexBy},
			ClusteringColumns: []string{id},
		}),
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
		Table: k.Table(fmt.Sprintf("%s_timeSeries_%s_%s_%s", name, timeField, idField, bucketSize), m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
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
		Table: k.Table(fmt.Sprintf("%s_timeSeries_%s_%s_%s_%s", name, indexField, timeField, idField, bucketSize), m, Keys{
			PartitionKeys:     []string{indexField, bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
		indexField: indexField,
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

// Returns table names in a keyspace
func (k *k) Tables() ([]string, error) {
	stmt := fmt.Sprintf("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='%s'", k.name)
	maps, err := k.qe.Query(stmt)
	if err != nil {
		return nil, err
	}
	ret := []string{}
	for _, m := range maps {
		ret = append(ret, m["columnfamily_name"].(string))
	}
	return ret, nil
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
	return k.qe.Execute(stmt)
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
