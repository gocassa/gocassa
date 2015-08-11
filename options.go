package gocassa

import (
	"time"
)

// ClusteringOrderColumn specifies a clustering column and whether its
// clustering order is ASC or DESC.
type ClusteringOrderColumn struct {
	Ascending bool
	Column    string
}

// ClusteringOrder specifies the clustering order property when creating a table
type ClusteringOrder struct {
	Columns []ClusteringOrderColumn
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
	// Order specifies the clustering order during table creation. If nil, it is omitted and the defaults are used.
	Order *ClusteringOrder
}

// Returns a new Options which is a right biased merge of the two initial Options.
func (o Options) Merge(neu Options) Options {
	ret := Options{
		TTL:       o.TTL,
		Limit:     o.Limit,
		TableName: o.TableName,
		Order:     o.Order,
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
	if neu.Order != nil {
		ret.Order = neu.Order
	}
	return ret
}

// AppendClusteringOrder adds a clustering order.  If there already clustering orders, the new one is added to the end.
func (o Options) AppendClusteringOrder(column string, ascending bool) Options {
	order := o.Order
	if order == nil {
		order = &ClusteringOrder{}
	}
	col := ClusteringOrderColumn{
		Column:    column,
		Ascending: ascending,
	}
	order.Columns = append(order.Columns, col)
	withOrder := Options{Order: order}

	return o.Merge(withOrder)
}
