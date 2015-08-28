package gocassa

import (
	"github.com/gocql/gocql"
	"time"
)

type ColumnDirection bool

const (
	ASC  ColumnDirection = false
	DESC                 = true
)

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
	// TableName
	TableName string
	// ClusteringOrder specifies the clustering order during table creation. If empty, it is omitted and the defaults are used.
	ClusteringOrder []ClusteringOrderColumn
	// Consistency specifies the consistency level. If nil, it is considered not set
	Consistency *gocql.Consistency
}

// Returns a new Options which is a right biased merge of the two initial Options.
func (o Options) Merge(neu Options) Options {
	ret := Options{
		TTL:             o.TTL,
		Limit:           o.Limit,
		TableName:       o.TableName,
		ClusteringOrder: o.ClusteringOrder,
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
	if neu.Consistency != nil {
		ret.Consistency = neu.Consistency
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
