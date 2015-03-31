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

func TTL(t time.Duration) Options {
	return Options{
		TTL: t,
	}
}

func (o Options) SetTTL(t time.Duration) Options {
	o.TTL = t
	return o
}

func Limit(i int) Options {
	return Options{
		Limit: i,
	}
}

func (o Options) SetLimit(i int) Options {
	o.Limit = i
	return o
}

func TableName(tableName string) Options {
	return Options{
		TableName: tableName,
	}
}

func (o Options) SetTableName(n string) Options {
	o.TableName = n
	return o
}

func (o Options) merge(neu Options) Options {
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
