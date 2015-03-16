package gocassa

import (
	"fmt"
	"strings"
	"time"
)

type k struct {
	qe        QueryExecutor
	name      string
	debugMode bool
}

// Connect to a certain keyspace directly. Same as using Connect().KeySpace(keySpaceName)
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

func (k *k) Table(name string, entity interface{}, keys Keys) Table {
	n := name + "__" + strings.Join(keys.PartitionKeys, "_") + "__" + strings.Join(keys.ClusteringColumns, "_")
	return k.rawTable(n, entity, keys)
}

func (k *k) rawTable(name string, entity interface{}, keys Keys) Table {
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

func (k *k) MapTable(name, id string, row interface{}) MapTable {
	return &mapT{
		t: k.rawTable(fmt.Sprintf("%s_map_%s", name, id), row, Keys{
			PartitionKeys: []string{id},
		}).(*t),
		idField: id,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) MultimapTable(name, fieldToIndexBy, id string, row interface{}) MultimapTable {
	return &multimapT{
		t: k.rawTable(fmt.Sprintf("%s_multimap_%s_%s", name, fieldToIndexBy, id), row, Keys{
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

// TimeSeriesTableWithName creates a TimeSeriesTable with an overridden table name
//
// This is needed as the current generator creates names which can exceed
// the 48 character table name limit.
// See https://github.com/hailocab/gocassa/issues/79
func (k *k) TimeSeriesTableWithName(tableName, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesT{
		t: k.table(tableName, row, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}).(*t),
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

func (k *k) MultiTimeSeriesTable(name, indexField, timeField, idField string, bucketSize time.Duration, row interface{}) MultiTimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &multiTimeSeriesT{
		t: k.table(fmt.Sprintf("%s_multiTimeSeries_%s_%s_%s_%s", name, indexField, timeField, idField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     []string{indexField, bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}).(*t),
		indexField: indexField,
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

// MultiTimeSeriesTableWithName creates a MultiTimeSeriesTable with an overridden table name
//
// This is needed as the current generator creates names which can exceed
// the 48 character table name limit.
// See https://github.com/hailocab/gocassa/issues/79
func (k *k) MultiTimeSeriesTableWithName(tableName, indexField, timeField, idField string, bucketSize time.Duration, row interface{}) MultiTimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &multiTimeSeriesT{
		t: k.table(tableName, row, m, Keys{
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
func (k *k) Tables() ([]string, error) {
	const stmt = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?"
	maps, err := k.qe.Query(stmt, k.name)
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

func (k *k) Name() string {
	return k.name
}
