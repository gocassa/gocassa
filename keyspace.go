package gocassa

import (
	"fmt"
	"strings"
	"time"
)

type tableFactory interface {
	NewTable(string, interface{}, map[string]interface{}, Keys) Table
}

type k struct {
	qe           QueryExecutor
	name         string
	debugMode    bool
	tableFactory tableFactory
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
	k.debugMode = b
}

func (k *k) Table(name string, entity interface{}, keys Keys) Table {
	n := name + "__" + strings.Join(keys.PartitionKeys, "_") + "__" + strings.Join(keys.ClusteringColumns, "_")
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return k.NewTable(n, entity, m, keys)
}

func (k *k) NewTable(name string, entity interface{}, fields map[string]interface{}, keys Keys) Table {
	// Act both as a proxy to a tableFactory, and as the tableFactory itself (in most situations, a k will be its own
	// tableFactory, but not always [ie. mocking])
	if k.tableFactory != k {
		return k.tableFactory.NewTable(name, entity, fields, keys)
	} else {
		ti := newTableInfo(k.name, name, keys, entity, fields)
		return &t{
			keySpace: k,
			info:     ti,
			options:  Options{},
		}
	}
}

func (k *k) MapTable(name, id string, row interface{}) MapTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	return &mapT{
		t: k.NewTable(fmt.Sprintf("%s_map_%s", name, id), row, m, Keys{
			PartitionKeys: []string{id},
		}),
		idField: id,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) MultimapTable(name, fieldToIndexBy, id string, row interface{}) MultimapTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	return &multimapT{
		t: k.NewTable(fmt.Sprintf("%s_multimap_%s_%s", name, fieldToIndexBy, id), row, m, Keys{
			PartitionKeys:     []string{fieldToIndexBy},
			ClusteringColumns: []string{id},
		}),
		idField:        id,
		fieldToIndexBy: fieldToIndexBy,
	}
}

func (k *k) MultimapMultiKeyTable(name string, fieldToIndexBy, id []string, row interface{}) MultimapMkTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	return &multimapMkT{
		t: k.NewTable(fmt.Sprintf("%s_multimapMk", name), row, m, Keys{
			PartitionKeys:     fieldToIndexBy,
			ClusteringColumns: id,
		}),
		idField:         id,
		fieldsToIndexBy: fieldToIndexBy,
	}
}

func (k *k) TimeSeriesTable(name, timeField, idField string, bucketSize time.Duration, row interface{}) TimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesT{
		t: k.NewTable(fmt.Sprintf("%s_timeSeries_%s_%s_%s", name, timeField, idField, bucketSize), row, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
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
		t: k.NewTable(fmt.Sprintf("%s_multiTimeSeries_%s_%s_%s_%s", name, indexField, timeField, idField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     []string{indexField, bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}),
		indexField: indexField,
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

func (k *k) MultiKeyTimeSeriesTable(name string, indexFields []string, timeField string, idFields []string, bucketSize time.Duration, row interface{}) MultiKeyTimeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}

	partitionKeys := indexFields
	partitionKeys = append(partitionKeys, bucketFieldName)
	clusteringColumns := []string{timeField}
	clusteringColumns = append(clusteringColumns, idFields...)

	m[bucketFieldName] = time.Now()
	return &multiKeyTimeSeriesT{
		t: k.NewTable(fmt.Sprintf("%s_multiKeyTimeSeries_%s_%s", name, timeField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     partitionKeys,
			ClusteringColumns: clusteringColumns,
		}),
		indexFields: indexFields,
		timeField:   timeField,
		idFields:    idFields,
		bucketSize:  bucketSize,
	}
}

func (k *k) FlakeSeriesTable(name, idField string, bucketSize time.Duration, row interface{}) FlakeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[flakeTimestampFieldName] = time.Now()
	m[bucketFieldName] = time.Now()
	return &flakeSeriesT{
		t: k.NewTable(fmt.Sprintf("%s_flakeSeries_%s_%s", name, idField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{flakeTimestampFieldName, idField},
		}),
		idField:    idField,
		bucketSize: bucketSize,
	}
}

func (k *k) MultiFlakeSeriesTable(name, indexField, idField string, bucketSize time.Duration, row interface{}) MultiFlakeSeriesTable {
	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[flakeTimestampFieldName] = time.Now()
	m[bucketFieldName] = time.Now()
	return &multiFlakeSeriesT{
		t: k.NewTable(fmt.Sprintf("%s_multiflakeSeries_%s_%s_%s", name, indexField, idField, bucketSize.String()), row, m, Keys{
			PartitionKeys:     []string{indexField, bucketFieldName},
			ClusteringColumns: []string{flakeTimestampFieldName, idField},
		}),
		idField:    idField,
		bucketSize: bucketSize,
		indexField: indexField,
	}
}

// Returns table names in a keyspace
func (k *k) Tables() ([]string, error) {
	const query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"

	if k.qe == nil {
		return nil, fmt.Errorf("no query executor configured")
	}

	stmt := newStatement(query, []interface{}{k.name})
	maps, err := k.qe.Query(stmt)
	if err != nil {
		return nil, err
	}
	ret := []string{}
	for _, m := range maps {
		ret = append(ret, m["table_name"].(string))
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
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.name, cf)
	stmt := newStatement(query, []interface{}{})
	return k.qe.Execute(stmt)
}

func (k *k) Name() string {
	return k.name
}
