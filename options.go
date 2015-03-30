package gocassa

import (
	"time"
)

// Options allows specification of (optional, hah) query parameters.
type Options struct {
	// TTL specifies a duration over which data is valid. It will be truncated to second precision upon statement
	// execution.
	TTL   time.Duration
	Limit int
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
