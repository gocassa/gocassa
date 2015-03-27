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
	k.debugMode = b
}

func (k *k) Table(name string, entity interface{}, keys Keys, opts ...TableOptions) Table {
	options := mergeTableOptions(opts...)

	// Construct a CQL table name if not provided
	if options.TableName == "" {
		options.TableName = name + "__" + strings.Join(keys.PartitionKeys, "_") + "__" + strings.Join(keys.ClusteringColumns, "_")
	}

	return k.rawTable(options.TableName, entity, keys)
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

func (k *k) MapTable(name, id string, row interface{}, opts ...TableOptions) MapTable {
	options := mergeTableOptions(opts...)

	// Construct a CQL table name if not provided
	if options.TableName == "" {
		options.TableName = fmt.Sprintf("%s_map_%s", name, id)
	}

	return &mapT{
		t: k.rawTable(options.TableName, row, Keys{
			PartitionKeys: []string{id},
		}).(*t),
		idField: id,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) MultimapTable(name, fieldToIndexBy, id string, row interface{}, opts ...TableOptions) MultimapTable {
	options := mergeTableOptions(opts...)

	// Construct a CQL table name if not provided
	if options.TableName == "" {
		options.TableName = fmt.Sprintf("%s_multimap_%s_%s", name, fieldToIndexBy, id)
	}

	return &multimapT{
		t: k.rawTable(options.TableName, row, Keys{
			PartitionKeys:     []string{fieldToIndexBy},
			ClusteringColumns: []string{id},
		}).(*t),
		idField:        id,
		fieldToIndexBy: fieldToIndexBy,
	}
}

func (k *k) TimeSeriesTable(name, timeField, idField string, bucketSize time.Duration, row interface{}, opts ...TableOptions) TimeSeriesTable {
	options := mergeTableOptions(opts...)

	// Construct a CQL table name if not provided
	if options.TableName == "" {
		options.TableName = fmt.Sprintf("%s_timeSeries_%s_%s_%s", name, timeField, idField, bucketSize.String())
	}

	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesT{
		t: k.table(options.TableName, row, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, idField},
		}).(*t),
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}
}

func (k *k) MultiTimeSeriesTable(name, indexField, timeField, idField string, bucketSize time.Duration, row interface{}, opts ...TableOptions) MultiTimeSeriesTable {
	options := mergeTableOptions(opts...)

	// Construct a CQL table name if not provided
	if options.TableName == "" {
		options.TableName = fmt.Sprintf("%s_multiTimeSeries_%s_%s_%s_%s", name, indexField, timeField, idField, bucketSize.String())
	}

	m, ok := toMap(row)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &multiTimeSeriesT{
		t: k.table(options.TableName, row, m, Keys{
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
