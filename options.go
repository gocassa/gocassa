package gocassa

import (
	"time"

	"github.com/gocql/gocql"
)

type ColumnDirection bool

const (
	ASC  ColumnDirection = false
	DESC                 = true
)

func (d ColumnDirection) String() string {
	switch d {
	case ASC:
		return "ASC"
	case DESC:
		return "DESC"
	default:
		return ""
	}
}

// ClusteringOrderColumn specifies a clustering column and whether its
// clustering order is ASC or DESC.
type ClusteringOrderColumn struct {
	Direction ColumnDirection
	Column    string
}

// Options can contain table or statement specific options.
// The reason for this is because statement specific (TTL, Limit) options make sense as table level options
// (eg. have default TTL for every Update without specifying it all the time)
type Options struct {
	// TTL specifies a duration over which data is valid. It will be truncated to second precision upon statement
	// execution.
	TTL time.Duration
	// Limit query result set
	Limit int
	// TableName overrides the default internal table name. When naming a table 'users' the internal table name becomes 'users_someTableSpecificMetaInformation'.
	TableName string
	// ClusteringOrder specifies the clustering order during table creation. If empty, it is omitted and the defaults are used.
	ClusteringOrder []ClusteringOrderColumn
	// Indicates if allow filtering should be appended at the end of the query
	AllowFiltering bool
	// Select allows you to do partial reads, ie. retrieve only a subset of fields
	Select []string
	// Consistency specifies the consistency level. If nil, it is considered not set
	Consistency *gocql.Consistency
	// Setting CompactStorage to true enables table creation with compact storage
	CompactStorage bool
	// Compressor specifies the compressor (if any) to use on a newly created table
	Compressor string
}

// Returns a new Options which is a right biased merge of the two initial Options.
func (o Options) Merge(neu Options) Options {
	ret := Options{
		TTL:             o.TTL,
		Limit:           o.Limit,
		TableName:       o.TableName,
		ClusteringOrder: o.ClusteringOrder,
		Select:          o.Select,
		CompactStorage:  o.CompactStorage,
		Compressor:      o.Compressor,
	}
	if neu.TTL != time.Duration(0) {
		ret.TTL = neu.TTL
	}
	if neu.Limit != 0 {
		ret.Limit = neu.Limit
	}
	if len(neu.TableName) > 0 {
		ret.TableName = neu.TableName
	}
	if neu.ClusteringOrder != nil {
		ret.ClusteringOrder = neu.ClusteringOrder
	}
	if neu.AllowFiltering {
		ret.AllowFiltering = neu.AllowFiltering
	}
	if len(neu.Select) > 0 {
		ret.Select = neu.Select
	}
	if neu.Consistency != nil {
		ret.Consistency = neu.Consistency
	}
	if neu.CompactStorage {
		ret.CompactStorage = neu.CompactStorage
	}
	if len(neu.Compressor) > 0 {
		ret.Compressor = neu.Compressor
	}
	return ret
}

// AppendClusteringOrder adds a clustering order.  If there already clustering orders, the new one is added to the end.
func (o Options) AppendClusteringOrder(column string, direction ColumnDirection) Options {
	col := ClusteringOrderColumn{
		Column:    column,
		Direction: direction,
	}
	co := append(o.ClusteringOrder, col)
	withOrder := Options{ClusteringOrder: co}

	return o.Merge(withOrder)
}
