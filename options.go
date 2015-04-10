package gocassa

import (
	"time"
)

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
}

// Returns a new Options which is a right biased merge of the two initial Options.
func (o Options) Merge(neu Options) Options {
	ret := Options{
		TTL:       o.TTL,
		Limit:     o.Limit,
		TableName: o.TableName,
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
	return ret
}
